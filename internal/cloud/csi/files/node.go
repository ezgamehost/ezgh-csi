package files

import (
	"context"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/mount-utils"

	cloudcsi "github.com/ezgamehost/ezgh-csi/internal/cloud/csi"
)

// mountOptions are every Files mount's; the PV's mountOptions follow them.
var mountOptions = []string{"vers=4.1", "hard", "noresvport", "rsize=1048576", "wsize=1048576", "timeo=600"}

// unmountTimeout bounds an unmount of a hard NFS mount whose server is gone, before it's forced.
const unmountTimeout = 30 * time.Second

// Node is the CSI Node service on each node (ezgh-csi-node files). It runs in the host's network
// namespace, so a TLS mount's handshake upcall reaches the node's tlshd.
type Node struct {
	csi.UnimplementedNodeServer
	// NodeName is the Node, ins-<id> (KUBE_NODE_NAME).
	NodeName string
	Mounter  mount.MounterForceUnmounter
}

func (n *Node) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{Capabilities: []*csi.NodeServiceCapability{{
		Type: &csi.NodeServiceCapability_Rpc{Rpc: &csi.NodeServiceCapability_RPC{Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME}},
	}}}, nil
}

// NodeGetInfo names the node's instance; a node mounts any number of Files volumes.
func (n *Node) NodeGetInfo(context.Context, *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	if !strings.HasPrefix(n.NodeName, "ins-") {
		return nil, status.Errorf(codes.FailedPrecondition, "node %q isn't named ins-<id>", n.NodeName)
	}
	return &csi.NodeGetInfoResponse{NodeId: "ins_" + strings.TrimPrefix(n.NodeName, "ins-")}, nil
}

// NodeStageVolume mounts the volume over NFS 4.1 from the mount target the controller picked.
func (n *Node) NodeStageVolume(_ context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	vc := req.GetVolumeCapability()
	if err := checkCapabilities([]*csi.VolumeCapability{vc}); err != nil {
		return nil, err
	}
	h, err := parseHandle(req.GetVolumeId())
	if err != nil {
		return nil, err
	}
	host := req.GetPublishContext()["hostname"]
	if host == "" || strings.ContainsAny(host, "/:, \t\n") {
		return nil, status.Errorf(codes.InvalidArgument, "publish_context hostname %q isn't a mount target's name", host)
	}
	tls, err := useTLS(req.GetVolumeContext())
	if err != nil {
		return nil, err
	}
	staging := req.GetStagingTargetPath()
	if staging == "" {
		return nil, status.Error(codes.InvalidArgument, "staging_target_path is required")
	}
	// A mount whose server went away (ESTALE, ENOTCONN, …) is unmounted and mounted again, not
	// refused on every retry.
	if _, err := n.Mounter.IsLikelyNotMountPoint(staging); mount.IsCorruptedMnt(err) {
		if err := n.Mounter.UnmountWithForce(staging, unmountTimeout); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	if mounted, err := cloudcsi.EnsureDir(n.Mounter, staging); err != nil || mounted {
		return &csi.NodeStageVolumeResponse{}, err
	}
	source := host + ":/" + h.accessPoint
	if h.accessPoint == "" {
		source = host + ":/" + h.fileSystem + h.path
	}
	opts := append([]string{}, mountOptions...)
	if tls {
		opts = append(opts, "xprtsec=tls")
	}
	opts = append(opts, vc.GetMount().GetMountFlags()...)
	if readOnly(vc) {
		opts = append(opts, "ro")
	}
	if err := n.Mounter.Mount(source, staging, "nfs4", opts); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &csi.NodeStageVolumeResponse{}, nil
}

func readOnly(vc *csi.VolumeCapability) bool {
	m := vc.GetAccessMode().GetMode()
	return m == csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY || m == csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY
}

func (n *Node) NodeUnstageVolume(_ context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	if req.GetStagingTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "staging_target_path is required")
	}
	if err := mount.CleanupMountWithForce(req.GetStagingTargetPath(), n.Mounter, false, unmountTimeout); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &csi.NodeUnstageVolumeResponse{}, nil
}

// NodePublishVolume bind-mounts the staged mount at the pod's target.
func (n *Node) NodePublishVolume(_ context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	vc := req.GetVolumeCapability()
	if err := checkCapabilities([]*csi.VolumeCapability{vc}); err != nil {
		return nil, err
	}
	if req.GetTargetPath() == "" || req.GetStagingTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "target_path and staging_target_path are required")
	}
	if mounted, err := cloudcsi.EnsureDir(n.Mounter, req.GetTargetPath()); err != nil || mounted {
		return &csi.NodePublishVolumeResponse{}, err
	}
	opts := []string{"bind"}
	if req.GetReadonly() || readOnly(vc) {
		opts = append(opts, "ro")
	}
	if err := n.Mounter.Mount(req.GetStagingTargetPath(), req.GetTargetPath(), "", opts); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &csi.NodePublishVolumeResponse{}, nil
}

func (n *Node) NodeUnpublishVolume(_ context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if req.GetTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "target_path is required")
	}
	if err := mount.CleanupMountWithForce(req.GetTargetPath(), n.Mounter, false, unmountTimeout); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}
