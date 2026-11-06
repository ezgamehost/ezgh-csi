package blockstore

import (
	"context"
	"path/filepath"
	"slices"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"k8s.io/mount-utils"
)

// mount-utils formats only on Linux.
func TestStageFormatsOnce(t *testing.T) {
	stage := func(blkid string, exit int, fs string) ([]string, string) {
		c := &cmds{out: map[string]struct {
			out  string
			exit int
		}{"blkid": {blkid, exit}}}
		n, fm := newNode(t, c)
		_, err := n.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
			VolumeId: "vol_aaaaaaaaaaaa", StagingTargetPath: filepath.Join(t.TempDir(), "staging"), VolumeCapability: mountCap(fs)[0]})
		if err != nil {
			t.Fatal(err)
		}
		log := fm.GetLog()
		if len(log) != 1 || log[0].Source != filepath.Join(n.DevDir, "virtio-vol_aaaaaaaaaaaa") {
			t.Fatal(log)
		}
		return c.ran, log[0].FSType
	}
	if ran, fs := stage("TYPE=ext4\n", 0, ""); slices.Contains(ran, "mkfs.ext4") || fs != "ext4" {
		t.Fatal("a formatted ext4 device was formatted again:", ran)
	}
	if ran, fs := stage("", 2, ""); !slices.Contains(ran, "mkfs.ext4") || fs != "ext4" {
		t.Fatal("an empty device wasn't formatted:", ran)
	}
	if ran, fs := stage("", 2, "xfs"); !slices.Contains(ran, "mkfs.xfs") || fs != "xfs" {
		t.Fatal(ran, fs)
	}
}

// Read-only access mounts read-only, and never formats a blank disk.
func TestStageReadOnly(t *testing.T) {
	vc := mountCap("")[0]
	vc.AccessMode.Mode = csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY
	stage := func(blkid string, exit int) (*cmds, *mount.FakeMounter, error) {
		c := &cmds{out: map[string]struct {
			out  string
			exit int
		}{"blkid": {blkid, exit}}}
		n, fm := newNode(t, c)
		_, err := n.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
			VolumeId: "vol_aaaaaaaaaaaa", StagingTargetPath: filepath.Join(t.TempDir(), "staging"), VolumeCapability: vc})
		return c, fm, err
	}
	if _, fm, err := stage("TYPE=ext4\n", 0); err != nil || !slices.Contains(must(fm.List())[0].Opts, "ro") {
		t.Fatal(err)
	}
	if c, _, err := stage("", 2); err == nil || slices.Contains(c.ran, "mkfs.ext4") {
		t.Fatal("formatted a blank disk read-only:", err, c.ran)
	}
}

func must[T any](v T, _ error) T { return v }
