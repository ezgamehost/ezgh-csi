package blockstore

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/mount-utils"
	"k8s.io/utils/exec"
	testingexec "k8s.io/utils/exec/testing"

	"github.com/ezgamehost/ezgh-csi/internal/cloud/ccm"
)

// cmds is a fake exec that records each command and answers it from out (name → output, exit code).
type cmds struct {
	ran []string
	out map[string]struct {
		out  string
		exit int
	}
}

func (c *cmds) exec() *testingexec.FakeExec {
	fe := &testingexec.FakeExec{}
	for range 10 {
		fe.CommandScript = append(fe.CommandScript, func(name string, args ...string) exec.Cmd {
			c.ran = append(c.ran, name)
			r := c.out[name]
			var err error
			if r.exit != 0 {
				err = &testingexec.FakeExitError{Status: r.exit}
			}
			return &testingexec.FakeCmd{CombinedOutputScript: []testingexec.FakeAction{func() ([]byte, []byte, error) { return []byte(r.out), nil, err }}}
		})
	}
	return fe
}

// newNode has a fake /dev/disk/by-id holding virtio-vol_aaaaaaaaaaaa → ../../vdb.
func newNode(t *testing.T, c *cmds) (*Node, *mount.FakeMounter) {
	dir := t.TempDir()
	byID := filepath.Join(dir, "disk", "by-id")
	if err := os.MkdirAll(byID, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "vdb"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../../vdb", filepath.Join(byID, "virtio-vol_aaaaaaaaaaaa")); err != nil {
		t.Fatal(err)
	}
	if c == nil {
		c = &cmds{}
	}
	fm := mount.NewFakeMounter(nil)
	return &Node{NodeName: "ins-bbbbbbbbbbbb", DevDir: byID, Mounter: mount.NewSafeFormatAndMount(fm, c.exec())}, fm
}

func TestDeviceByIDNeverVdX(t *testing.T) {
	n, _ := newNode(t, nil)
	dev, err := n.device("vol_aaaaaaaaaaaa")
	if err != nil || dev != filepath.Join(n.DevDir, "virtio-vol_aaaaaaaaaaaa") {
		t.Fatal(dev, err)
	}
	for _, bad := range []string{"vol_cccccccccccc", "vdb", "vol_../../vdb"} {
		if _, err := n.device(bad); code(err) != codes.NotFound {
			t.Fatal(bad, err)
		}
	}
}

func blockCap() *csi.VolumeCapability {
	return &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
	}
}

func TestBlockModeBindsDevice(t *testing.T) {
	n, fm := newNode(t, nil)
	target := filepath.Join(t.TempDir(), "pods", "p1", "volumeDevices", "vol")
	_, err := n.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId: "vol_aaaaaaaaaaaa", TargetPath: target, StagingTargetPath: "/staging", VolumeCapability: blockCap()})
	if err != nil {
		t.Fatal(err)
	}
	log := fm.GetLog()
	if len(log) != 1 || log[0].Source != filepath.Join(n.DevDir, "virtio-vol_aaaaaaaaaaaa") || log[0].Target != realPath(t, target) {
		t.Fatal(log)
	}
	if fi, err := os.Stat(target); err != nil || fi.IsDir() {
		t.Fatal("a block volume's target is a file", err)
	}
	// Staging a block volume formats and mounts nothing.
	if _, err := n.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId: "vol_aaaaaaaaaaaa", StagingTargetPath: t.TempDir(), VolumeCapability: blockCap()}); err != nil || len(fm.GetLog()) != 1 {
		t.Fatal(err, fm.GetLog())
	}
}

func TestPublishBindsStaging(t *testing.T) {
	n, fm := newNode(t, nil)
	target := filepath.Join(t.TempDir(), "mount")
	_, err := n.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId: "vol_aaaaaaaaaaaa", TargetPath: target, StagingTargetPath: "/staging", Readonly: true, VolumeCapability: mountCap("")[0]})
	if err != nil {
		t.Fatal(err)
	}
	if log := fm.GetLog(); len(log) != 1 || log[0].Source != "/staging" || log[0].Target != realPath(t, target) {
		t.Fatal(log)
	}
	if mps, _ := fm.List(); len(mps) != 1 || mps[0].Opts[0] != "bind" || mps[0].Opts[1] != "ro" {
		t.Fatal(mps)
	}
}

func TestPublishReadOnlyMode(t *testing.T) {
	n, fm := newNode(t, nil)
	vc := mountCap("")[0]
	vc.AccessMode.Mode = csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY
	if _, err := n.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId: "vol_aaaaaaaaaaaa", TargetPath: filepath.Join(t.TempDir(), "m"), StagingTargetPath: "/staging", VolumeCapability: vc}); err != nil {
		t.Fatal(err)
	}
	if mps, _ := fm.List(); len(mps) != 1 || len(mps[0].Opts) != 2 || mps[0].Opts[1] != "ro" {
		t.Fatal(mps)
	}
}

// A block volume's expand has nothing to grow, even when the kubelet sends no capability.
func TestExpandBlockDeviceWithoutCapability(t *testing.T) {
	c := &cmds{}
	n, _ := newNode(t, c)
	dev := filepath.Join(t.TempDir(), "dev")
	if err := os.WriteFile(dev, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := n.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{VolumeId: "vol_aaaaaaaaaaaa", VolumePath: dev}); err != nil || len(c.ran) != 0 {
		t.Fatal(err, c.ran)
	}
}

func TestMaxVolumesFromLabel(t *testing.T) {
	n, _ := newNode(t, nil)
	n.Nodes = fake.NewClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "ins-bbbbbbbbbbbb", Labels: map[string]string{ccm.MaxVolumesLabel: "8"}}})
	info, err := n.NodeGetInfo(context.Background(), nil)
	if err != nil || info.NodeId != "ins_bbbbbbbbbbbb" || info.MaxVolumesPerNode != 8 {
		t.Fatal(info, err)
	}
	// 0 would read as "no limit" to the kubelet.
	n.Nodes = fake.NewClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "ins-bbbbbbbbbbbb", Labels: map[string]string{ccm.MaxVolumesLabel: "0"}}})
	if _, err := n.NodeGetInfo(context.Background(), nil); code(err) != codes.FailedPrecondition {
		t.Fatal(err)
	}
	// Not labelled yet: waits, then Unavailable.
	labelWait = 50 * time.Millisecond
	n.Nodes = fake.NewClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "ins-bbbbbbbbbbbb"}})
	if _, err := n.NodeGetInfo(context.Background(), nil); code(err) != codes.Unavailable {
		t.Fatal(err)
	}
}

// realPath is p with symlinks resolved, as FakeMounter records targets (/var is /private/var on macOS).
func realPath(t *testing.T, p string) string {
	r, err := filepath.EvalSymlinks(p)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
