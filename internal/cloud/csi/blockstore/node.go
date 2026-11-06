package blockstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/mount-utils"

	"github.com/ezgamehost/ezgh-csi/internal/cloud/ccm"
	cloudcsi "github.com/ezgamehost/ezgh-csi/internal/cloud/csi"
)

// labelWait is how long NodeGetInfo waits for the CCM's max-volumes label on a Node that just
// joined.
var labelWait = 60 * time.Second

// Node is the CSI Node service on each node (ezgh-csi-node blockstore).
type Node struct {
	csi.UnimplementedNodeServer
	// NodeName is the Node, ins-<id> (KUBE_NODE_NAME).
	NodeName string
	Nodes    kubernetes.Interface
	Mounter  *mount.SafeFormatAndMount
	// DevDir is /dev/disk/by-id, where compute's volumes appear as virtio-vol_….
	DevDir string
}

func (n *Node) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	out := &csi.NodeGetCapabilitiesResponse{}
	for _, t := range []csi.NodeServiceCapability_RPC_Type{
		csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME, csi.NodeServiceCapability_RPC_EXPAND_VOLUME, csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
	} {
		out.Capabilities = append(out.Capabilities, &csi.NodeServiceCapability{
			Type: &csi.NodeServiceCapability_Rpc{Rpc: &csi.NodeServiceCapability_RPC{Type: t}}})
	}
	return out, nil
}

// NodeGetInfo names the node's instance and how many volumes it takes beside its root volume
// (the CCM's ccm.MaxVolumesLabel).
func (n *Node) NodeGetInfo(ctx context.Context, _ *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	if !strings.HasPrefix(n.NodeName, "ins-") {
		return nil, status.Errorf(codes.FailedPrecondition, "node %q isn't named ins-<id>", n.NodeName)
	}
	deadline := time.Now().Add(labelWait)
	for {
		node, err := n.Nodes.CoreV1().Nodes().Get(ctx, n.NodeName, metav1.GetOptions{})
		if err == nil {
			if v, ok := node.Labels[ccm.MaxVolumesLabel]; ok {
				// 0 would mean "no limit" to the kubelet: a type that takes no data volumes is an error.
				limit, err := strconv.Atoi(v)
				if err != nil || limit < 1 {
					return nil, status.Errorf(codes.FailedPrecondition, "label %s is %q: this node takes no Block Storage volumes", ccm.MaxVolumesLabel, v)
				}
				return &csi.NodeGetInfoResponse{NodeId: "ins_" + strings.TrimPrefix(n.NodeName, "ins-"), MaxVolumesPerNode: int64(limit)}, nil
			}
		}
		if sleep(ctx, deadline) != nil {
			return nil, status.Errorf(codes.Unavailable, "Node %s has no %s label yet", n.NodeName, ccm.MaxVolumesLabel)
		}
	}
}

// device is the volume's disk by its ID, never /dev/vdX: device letters change across attaches.
func (n *Node) device(volume string) (string, error) {
	if !strings.HasPrefix(volume, "vol_") || strings.ContainsAny(volume, "/.") {
		return "", status.Errorf(codes.NotFound, "volume %q doesn't exist", volume)
	}
	dev := filepath.Join(n.DevDir, "virtio-"+volume)
	if _, err := os.Stat(dev); err != nil {
		return "", status.Errorf(codes.NotFound, "%s isn't present: volume %s isn't attached to this node", dev, volume)
	}
	return dev, nil
}

func (n *Node) NodeStageVolume(_ context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	vc := req.GetVolumeCapability()
	if err := checkCapabilities([]*csi.VolumeCapability{vc}); err != nil {
		return nil, err
	}
	if req.GetStagingTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "staging_target_path is required")
	}
	dev, err := n.device(req.GetVolumeId())
	if err != nil {
		return nil, err
	}
	if vc.GetBlock() != nil {
		return &csi.NodeStageVolumeResponse{}, nil // published straight from the device
	}
	if mounted, err := cloudcsi.EnsureDir(n.Mounter, req.GetStagingTargetPath()); err != nil || mounted {
		return &csi.NodeStageVolumeResponse{}, err
	}
	fs := vc.GetMount().GetFsType()
	if fs == "" {
		fs = "ext4"
	}
	opts := vc.GetMount().GetMountFlags()
	if readOnly(vc) {
		opts = append(opts, "ro") // and SafeFormatAndMount refuses to format a blank disk read-only
	}
	// SafeFormatAndMount formats only a disk blkid finds empty.
	if err := n.Mounter.FormatAndMount(dev, req.GetStagingTargetPath(), fs, opts); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &csi.NodeStageVolumeResponse{}, nil
}

func readOnly(vc *csi.VolumeCapability) bool {
	return vc.GetAccessMode().GetMode() == csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY
}

func (n *Node) NodeUnstageVolume(_ context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	if req.GetStagingTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "staging_target_path is required")
	}
	if err := mount.CleanupMountPoint(req.GetStagingTargetPath(), n.Mounter, false); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (n *Node) NodePublishVolume(_ context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	vc, target := req.GetVolumeCapability(), req.GetTargetPath()
	if err := checkCapabilities([]*csi.VolumeCapability{vc}); err != nil {
		return nil, err
	}
	if target == "" {
		return nil, status.Error(codes.InvalidArgument, "target_path is required")
	}
	opts := []string{"bind"}
	if req.GetReadonly() || readOnly(vc) {
		opts = append(opts, "ro")
	}
	source := req.GetStagingTargetPath()
	if vc.GetBlock() != nil {
		// volumeMode: Block binds the device file itself onto a file at target.
		dev, err := n.device(req.GetVolumeId())
		if err != nil {
			return nil, err
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		f, err := os.OpenFile(target, os.O_CREATE, 0o640)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		_ = f.Close()
		if notMounted, err := n.Mounter.IsLikelyNotMountPoint(target); err != nil || !notMounted {
			return &csi.NodePublishVolumeResponse{}, err
		}
		source = dev
	} else {
		if source == "" {
			return nil, status.Error(codes.InvalidArgument, "staging_target_path is required")
		}
		if mounted, err := cloudcsi.EnsureDir(n.Mounter, target); err != nil || mounted {
			return &csi.NodePublishVolumeResponse{}, err
		}
	}
	if err := n.Mounter.Mount(source, target, "", opts); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &csi.NodePublishVolumeResponse{}, nil
}

func (n *Node) NodeUnpublishVolume(_ context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if req.GetTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "target_path is required")
	}
	if err := mount.CleanupMountPoint(req.GetTargetPath(), n.Mounter, true); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// NodeExpandVolume grows the file system to the disk, which the controller already grew.
func (n *Node) NodeExpandVolume(_ context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	if req.GetVolumePath() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_path is required")
	}
	// A block volume (said, or a device file at volume_path) has no file system to grow.
	if fi, err := os.Stat(req.GetVolumePath()); req.GetVolumeCapability().GetBlock() != nil || (err == nil && !fi.IsDir()) {
		return &csi.NodeExpandVolumeResponse{}, nil
	}
	dev, err := n.device(req.GetVolumeId())
	if err != nil {
		return nil, err
	}
	if _, err := mount.NewResizeFs(n.Mounter.Exec).Resize(dev, req.GetVolumePath()); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &csi.NodeExpandVolumeResponse{}, nil
}

func (n *Node) NodeGetVolumeStats(_ context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	path := req.GetVolumePath()
	fi, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) || path == "" {
		return nil, status.Errorf(codes.NotFound, "%q doesn't exist", path)
	}
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if !fi.IsDir() {
		// A block volume: its size is all there is to say.
		out, err := n.Mounter.Exec.Command("blockdev", "--getsize64", path).CombinedOutput()
		size, perr := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
		if err != nil || perr != nil {
			return nil, status.Errorf(codes.Internal, "blockdev %s: %v %s", path, err, out)
		}
		return &csi.NodeGetVolumeStatsResponse{Usage: []*csi.VolumeUsage{{Unit: csi.VolumeUsage_BYTES, Total: size}}}, nil
	}
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	bs := int64(st.Bsize)
	return &csi.NodeGetVolumeStatsResponse{Usage: []*csi.VolumeUsage{
		{Unit: csi.VolumeUsage_BYTES, Total: int64(st.Blocks) * bs, Available: int64(st.Bavail) * bs, Used: int64(st.Blocks-st.Bfree) * bs},
		{Unit: csi.VolumeUsage_INODES, Total: int64(st.Files), Available: int64(st.Ffree), Used: int64(st.Files - st.Ffree)},
	}}, nil
}
