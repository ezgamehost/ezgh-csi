package files

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"k8s.io/mount-utils"
)

// forceMounter is the fake mounter with Linux's forced unmount, counted.
type forceMounter struct {
	*mount.FakeMounter
	forced int
}

func (f *forceMounter) UnmountWithForce(target string, _ time.Duration) error {
	f.forced++
	delete(f.MountCheckErrors, target) // the stale mount is gone
	return f.Unmount(target)
}

func newNode() (*Node, *forceMounter) {
	fm := &forceMounter{FakeMounter: mount.NewFakeMounter(nil)}
	return &Node{NodeName: "ins-bbbbbbbbbbbb", Mounter: fm}, fm
}

// tempDir is t.TempDir() with symlinks resolved, as the fake mounter records paths (macOS /var).
func tempDir(t *testing.T) string {
	d, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func stage(t *testing.T, n *Node, id string, vc map[string]string, flags []string, mode csi.VolumeCapability_AccessMode_Mode) (string, error) {
	dir := filepath.Join(tempDir(t), "staging")
	_, err := n.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId: id, StagingTargetPath: dir, VolumeContext: vc,
		PublishContext: map[string]string{"hostname": "b.fs.us-west-1.ezghcompute.com"},
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{MountFlags: flags}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: mode},
		},
	})
	return dir, err
}

func TestMountOptionsTLS(t *testing.T) {
	const base = "vers=4.1,hard,noresvport,rsize=1048576,wsize=1048576,timeo=600"
	for _, tc := range []struct {
		name, id string
		vc       map[string]string
		flags    []string
		mode     csi.VolumeCapability_AccessMode_Mode
		source   string
		opts     string
	}{
		{"plain", "fap_aaaaaaaaaaaa", nil, nil, csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			"b.fs.us-west-1.ezghcompute.com:/fap_aaaaaaaaaaaa", base},
		{"tls", "fap_aaaaaaaaaaaa", map[string]string{"tls": "true"}, nil, csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			"b.fs.us-west-1.ezghcompute.com:/fap_aaaaaaaaaaaa", base + ",xprtsec=tls"},
		{"static folder, PV mountOptions, read-only", "fsy_aaaaaaaaaaaa:/shared/web", map[string]string{"tls": "false"}, []string{"noac"},
			csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY, "b.fs.us-west-1.ezghcompute.com:/fsy_aaaaaaaaaaaa/shared/web", base + ",noac,ro"},
		{"static file system", "fsy_aaaaaaaaaaaa", map[string]string{"tls": "true"}, nil, csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			"b.fs.us-west-1.ezghcompute.com:/fsy_aaaaaaaaaaaa", base + ",xprtsec=tls"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n, fm := newNode()
			dir, err := stage(t, n, tc.id, tc.vc, tc.flags, tc.mode)
			if err != nil {
				t.Fatal(err)
			}
			if len(fm.MountPoints) != 1 {
				t.Fatal(fm.MountPoints)
			}
			mp := fm.MountPoints[0]
			if mp.Device != tc.source || mp.Path != dir || mp.Type != "nfs4" || strings.Join(mp.Opts, ",") != tc.opts {
				t.Fatalf("%+v", mp)
			}
			// Staging again finds it mounted.
			if _, err := n.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
				VolumeId: tc.id, StagingTargetPath: dir, PublishContext: map[string]string{"hostname": "b.fs.us-west-1.ezghcompute.com"},
				VolumeCapability: rwx[0],
			}); err != nil || len(fm.MountPoints) != 1 {
				t.Fatal(err, fm.MountPoints)
			}
		})
	}
}

func TestStageRefuses(t *testing.T) {
	n, fm := newNode()
	for _, id := range []string{"fsy_aaaaaaaaaaaa:/a/../../etc", "fsy_aaaaaaaaaaaa:..", "vol_aaaaaaaaaaaa"} {
		if _, err := stage(t, n, id, nil, nil, csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER); code(err) != codes.InvalidArgument {
			t.Fatal(id, err)
		}
	}
	// A static volume's tls attribute is "true" or "false".
	if _, err := stage(t, n, "fsy_aaaaaaaaaaaa", map[string]string{"tls": "yes"}, nil, csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER); code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	// No mount target hostname from the controller.
	if _, err := n.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId: "fap_aaaaaaaaaaaa", StagingTargetPath: t.TempDir(), VolumeCapability: rwx[0],
	}); code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	if len(fm.MountPoints) != 0 {
		t.Fatal(fm.MountPoints)
	}
}

func TestPublishBindsStaging(t *testing.T) {
	n, fm := newNode()
	staging, err := stage(t, n, "fap_aaaaaaaaaaaa", nil, nil, csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER)
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(tempDir(t), "target")
	if _, err := n.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId: "fap_aaaaaaaaaaaa", StagingTargetPath: staging, TargetPath: target, VolumeCapability: rwx[0], Readonly: true,
	}); err != nil {
		t.Fatal(err)
	}
	mp := fm.MountPoints[1]
	if mp.Path != target || mp.Device != "b.fs.us-west-1.ezghcompute.com:/fap_aaaaaaaaaaaa" || strings.Join(mp.Opts, ",") != "bind,ro" {
		t.Fatalf("%+v", mp)
	}
	if _, err := n.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{VolumeId: "fap_aaaaaaaaaaaa", TargetPath: target}); err != nil {
		t.Fatal(err)
	}
	if _, err := n.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{VolumeId: "fap_aaaaaaaaaaaa", StagingTargetPath: staging}); err != nil || len(fm.MountPoints) != 0 {
		t.Fatal(err, fm.MountPoints)
	}
}

func TestNodeGetInfo(t *testing.T) {
	n, _ := newNode()
	r, err := n.NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})
	if err != nil || r.NodeId != "ins_bbbbbbbbbbbb" || r.MaxVolumesPerNode != 0 {
		t.Fatal(r, err)
	}
}

// A staged mount whose server went away is unmounted (forced) and mounted again.
func TestStageRemountsCorrupted(t *testing.T) {
	n, fm := newNode()
	dir := filepath.Join(tempDir(t), "staging")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	fm.MountPoints = []mount.MountPoint{{Device: "old:/fap_aaaaaaaaaaaa", Path: dir, Type: "nfs4"}}
	fm.MountCheckErrors = map[string]error{dir: &os.PathError{Op: "stat", Path: dir, Err: syscall.ESTALE}}
	if _, err := n.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId: "fap_aaaaaaaaaaaa", StagingTargetPath: dir, VolumeCapability: rwx[0],
		PublishContext: map[string]string{"hostname": "b.fs.us-west-1.ezghcompute.com"},
	}); err != nil {
		t.Fatal(err)
	}
	if fm.forced != 1 || len(fm.MountPoints) != 1 || fm.MountPoints[0].Device != "b.fs.us-west-1.ezghcompute.com:/fap_aaaaaaaaaaaa" {
		t.Fatal(fm.forced, fm.MountPoints)
	}
}
