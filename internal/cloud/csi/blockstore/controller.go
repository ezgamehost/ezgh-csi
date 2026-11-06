package blockstore

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/ezgamehost/ezgh-csi/internal/cloud/config"
	cloudcsi "github.com/ezgamehost/ezgh-csi/internal/cloud/csi"
	"github.com/ezgamehost/ezgh-csi/internal/ezghclient"
	"github.com/ezgamehost/ezgh-csi/internal/ezghclient/blockstoreapi"
	"github.com/ezgamehost/ezgh-csi/internal/ezghclient/computeapi"
)

// Controller is the CSI Controller service. The kubernetes-csi sidecars on the same VM call it,
// and hold the leader election; it acts on EZGH's APIs as the cluster's Bot.
type Controller struct {
	csi.UnimplementedControllerServer
	Blockstore *blockstoreapi.Client
	Compute    *computeapi.Client
	// Nodes reads Node objects live, through the ezgh:cloud kubeconfig. Nothing is cached.
	Nodes kubernetes.Interface
	// Config reads cloud.json on each call.
	Config func() (config.Config, error)
}

const (
	gib            = int64(1) << 30
	fstypeKey      = cloudcsi.FstypeKey
	metadataPrefix = cloudcsi.MetadataPrefix
)

// How long a call follows a volume or an operation before answering Aborted for the sidecar to
// retry. The sidecars' --timeout is longer than these.
var (
	pollEvery  = 2 * time.Second
	createWait = 30 * time.Second
	attachWait = 60 * time.Second
)

var controllerCaps = []csi.ControllerServiceCapability_RPC_Type{
	csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
	csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
	csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
	csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
	csi.ControllerServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER,
}

func (c *Controller) ControllerGetCapabilities(context.Context, *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	out := &csi.ControllerGetCapabilitiesResponse{}
	for _, t := range controllerCaps {
		out.Capabilities = append(out.Capabilities, &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{Rpc: &csi.ControllerServiceCapability_RPC{Type: t}}})
	}
	return out, nil
}

func (c *Controller) config() (config.Config, error) {
	cfg, err := c.Config()
	if err != nil {
		return cfg, status.Error(codes.Unavailable, "cluster configuration: "+err.Error())
	}
	return cfg, nil
}

func (c *Controller) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	if err := checkCapabilities(req.GetVolumeCapabilities()); err != nil {
		return nil, err
	}
	cfg, err := c.config()
	if err != nil {
		return nil, err
	}
	typ := "gp1"
	body := blockstoreapi.VolumeCreate{Name: req.GetName(), Source: blockstoreapi.CreateSource{Kind: "empty"}}
	for k, v := range req.GetParameters() {
		switch k {
		case "type":
			typ = v
		case "iops", "throughputMBps":
			n, err := strconv.Atoi(v)
			if err != nil || n <= 0 {
				return nil, status.Errorf(codes.InvalidArgument, "parameter %s must be a positive whole number", k)
			}
			if k == "iops" {
				body.IOPS = &n
			} else {
				body.ThroughputMBps = &n
			}
		case fstypeKey:
			if err := checkFstype(v); err != nil {
				return nil, err
			}
		default:
			if !strings.HasPrefix(k, metadataPrefix) {
				return nil, status.Errorf(codes.InvalidArgument, "unknown parameter %q (type, iops, throughputMBps)", k)
			}
		}
	}
	body.Tags = cloudcsi.Tags(cfg, req.GetParameters())
	minBytes := int64(0)
	if src := req.GetVolumeContentSource(); src != nil {
		snap := src.GetSnapshot()
		if snap == nil {
			return nil, status.Error(codes.InvalidArgument, "volumes can't be cloned; restore from a VolumeSnapshot instead")
		}
		if !strings.HasPrefix(snap.GetSnapshotId(), "snp_") {
			return nil, status.Errorf(codes.NotFound, "snapshot %q doesn't exist", snap.GetSnapshotId())
		}
		s, err := c.Blockstore.GetSnapshot(ctx, cfg.Org, cfg.Project, snap.GetSnapshotId())
		if err != nil {
			return nil, cloudcsi.APIStatus(err, nil)
		}
		// A restore keeps the snapshot's type and is at least its volume's size.
		typ = s.VolumeType
		minBytes, _ = strconv.ParseInt(s.VolumeSizeBytes, 10, 64)
		body.Source = blockstoreapi.CreateSource{Kind: "snapshot", SnapshotID: s.Slug}
	}
	types, err := c.Blockstore.ListVolumeTypes(ctx, cfg.Org)
	if err != nil {
		return nil, cloudcsi.APIStatus(err, nil)
	}
	var vt *blockstoreapi.VolumeType
	for i := range types {
		if types[i].Name == typ {
			vt = &types[i]
		}
	}
	if vt == nil {
		return nil, status.Errorf(codes.InvalidArgument, "volume type %q doesn't exist", typ)
	}
	size, err := sizeFor(req.GetCapacityRange(), minBytes, vt.SizeGiB.Min, vt.SizeGiB.Max)
	if err != nil {
		return nil, err
	}
	body.VolumeType, body.SizeBytes = typ, strconv.FormatInt(size, 10)
	v, err := c.createVolume(ctx, cfg, req.GetName(), body)
	if err != nil {
		return nil, err
	}
	return &csi.CreateVolumeResponse{Volume: &csi.Volume{
		VolumeId: v.Slug, CapacityBytes: bytes(v.SizeBytes), ContentSource: req.GetVolumeContentSource(),
	}}, nil
}

// createLinks is how many volumes one name may go through when they fail.
const createLinks = 3

// createVolume creates the volume once per name: the CO retries under the same name (pvc-<uid>),
// and the Idempotency-Key csi-<name> gets the same volume back. A volume that failed (or is gone)
// (or being deleted) is deleted and replaced under csi-<name>-<its vol_>, so every retry walks the same chain and
// none creates a second volume.
func (c *Controller) createVolume(ctx context.Context, cfg config.Config, name string, body blockstoreapi.VolumeCreate) (*blockstoreapi.Volume, error) {
	key := "csi-" + name
	for link := 1; ; link++ {
		op, err := c.Blockstore.CreateVolume(ezghclient.WithIdempotencyKey(ctx, key), cfg.Org, cfg.Project, body)
		if err != nil {
			return nil, cloudcsi.APIStatus(err, createConflicts)
		}
		id := op.Volume.Slug
		v, err := c.waitVolume(ctx, cfg, id, createWait, func(v *blockstoreapi.Volume) bool { return v.Status != "creating" })
		gone := status.Code(err) == codes.NotFound
		if err != nil && !gone {
			return nil, err
		}
		// failed, or replaced already (an earlier call deleted it): on to the next link.
		if !gone && v.Status != "failed" && v.Status != "deleting" {
			return v, nil
		}
		if link == createLinks {
			reason := "it no longer exists"
			if !gone && v.FailureReason != nil {
				reason = *v.FailureReason
			}
			return nil, status.Errorf(codes.Internal, "volume %s failed (%s), as did the %d before it", id, reason, createLinks-1)
		}
		if !gone && v.Status == "failed" {
			_, _ = c.Blockstore.DeleteVolume(ctx, cfg.Org, cfg.Project, id) // best effort; the volume is tagged with the cluster
		}
		key = "csi-" + name + "-" + id
	}
}

// createConflicts are a create's 409s.
var createConflicts = map[string]codes.Code{
	"insufficient_capacity":   codes.ResourceExhausted,
	"snapshot_not_ready":      codes.Unavailable,
	"idempotency_conflict":    codes.AlreadyExists,
	"idempotency_in_progress": codes.Aborted,
}

// sizeFor is the request rounded up to whole GiB, at least minBytes and the type's minimum.
func sizeFor(r *csi.CapacityRange, minBytes int64, minGiB, maxGiB int) (int64, error) {
	n := (max(r.GetRequiredBytes(), minBytes) + gib - 1) / gib
	n = max(n, int64(minGiB), 1)
	if maxGiB > 0 && n > int64(maxGiB) {
		return 0, status.Errorf(codes.OutOfRange, "the volume type takes at most %d GiB", maxGiB)
	}
	if l := r.GetLimitBytes(); l > 0 && n*gib > l {
		return 0, status.Errorf(codes.OutOfRange, "%d GiB (whole GiB, at least the type's minimum) is over the limit of %d bytes", n, l)
	}
	return n * gib, nil
}

func bytes(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

// waitVolume reads the volume until done or failed, for up to limit.
func (c *Controller) waitVolume(ctx context.Context, cfg config.Config, id string, limit time.Duration, done func(*blockstoreapi.Volume) bool) (*blockstoreapi.Volume, error) {
	deadline := time.Now().Add(limit)
	for {
		v, err := c.Blockstore.GetVolume(ctx, cfg.Org, cfg.Project, id)
		if err != nil {
			return nil, cloudcsi.APIStatus(err, nil)
		}
		if v.Status == "failed" || done(v) {
			return v, nil
		}
		if err := sleep(ctx, deadline); err != nil {
			return nil, status.Errorf(codes.Aborted, "volume %s is still %s", id, v.Status)
		}
	}
}

// sleep waits pollEvery, or fails when that passes deadline or ctx ends.
func sleep(ctx context.Context, deadline time.Time) error {
	if time.Now().Add(pollEvery).After(deadline) {
		return context.DeadlineExceeded
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(pollEvery):
		return nil
	}
}

func (c *Controller) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if !strings.HasPrefix(req.GetVolumeId(), "vol_") {
		return &csi.DeleteVolumeResponse{}, nil
	}
	cfg, err := c.config()
	if err != nil {
		return nil, err
	}
	// Only this cluster's volume: a PV copied into another cluster of the project (a restored
	// backup) must not delete the first cluster's data.
	v, err := c.Blockstore.GetVolume(ctx, cfg.Org, cfg.Project, req.GetVolumeId())
	if ezghclient.IsNotFound(err) {
		return &csi.DeleteVolumeResponse{}, nil
	}
	if err != nil {
		return nil, cloudcsi.APIStatus(err, nil)
	}
	if !cloudcsi.Owned(cfg, v.Tags) {
		return &csi.DeleteVolumeResponse{}, nil
	}
	if _, err := c.Blockstore.DeleteVolume(ctx, cfg.Org, cfg.Project, req.GetVolumeId()); err != nil && !ezghclient.IsNotFound(err) {
		return nil, cloudcsi.APIStatus(err, map[string]codes.Code{"volume_in_use": codes.FailedPrecondition})
	}
	return &csi.DeleteVolumeResponse{}, nil
}

func (c *Controller) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	vol, node := req.GetVolumeId(), req.GetNodeId()
	if !strings.HasPrefix(vol, "vol_") || !strings.HasPrefix(node, "ins_") {
		return nil, status.Errorf(codes.NotFound, "volume %q or node %q doesn't exist", vol, node)
	}
	if err := checkCapabilities([]*csi.VolumeCapability{req.GetVolumeCapability()}); err != nil {
		return nil, err
	}
	cfg, err := c.config()
	if err != nil {
		return nil, err
	}
	_, op, err := c.Compute.Attachment(ctx, cfg.Org, cfg.Project, node, "attach-volume", computeapi.VolumeAttach{VolumeID: vol})
	if err != nil {
		return nil, cloudcsi.APIStatus(err, map[string]codes.Code{
			"volume_limit_exceeded":     codes.ResourceExhausted,
			"instance_restart_required": codes.FailedPrecondition,
			"volume_detaching":          codes.Aborted,
			"volume_in_use":             codes.FailedPrecondition,
		})
	}
	if err := c.waitOperation(ctx, cfg, op); err != nil {
		return nil, err
	}
	return &csi.ControllerPublishVolumeResponse{}, nil
}

func (c *Controller) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	vol, node := req.GetVolumeId(), req.GetNodeId()
	if !strings.HasPrefix(vol, "vol_") || !strings.HasPrefix(node, "ins_") {
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}
	cfg, err := c.config()
	if err != nil {
		return nil, err
	}
	v, err := c.Blockstore.GetVolume(ctx, cfg.Org, cfg.Project, vol)
	if ezghclient.IsNotFound(err) {
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}
	if err != nil {
		return nil, cloudcsi.APIStatus(err, nil)
	}
	if v.Attachment == nil || v.Attachment.Instance != node {
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}
	force, err := c.instanceGone(ctx, cfg, node)
	if err != nil {
		return nil, err
	}
	_, op, err := c.Compute.Attachment(ctx, cfg.Org, cfg.Project, node, "detach-volume", computeapi.VolumeDetach{VolumeID: vol, Force: force})
	if ezghclient.IsNotFound(err) {
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}
	if err != nil {
		return nil, cloudcsi.APIStatus(err, map[string]codes.Code{"volume_detaching": codes.Aborted, "instance_restart_required": codes.FailedPrecondition})
	}
	if err := c.waitOperation(ctx, cfg, op); err != nil {
		return nil, err
	}
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

// instanceGone is when a detach may be forced: the Node ins-<id> is gone AND the instance is
// gone (compute answers 404 once an instance is terminated; a terminated read is taken the
// same). A Node a customer deleted while its instance runs detaches normally, so the guest
// releases the disk and loses no writes.
func (c *Controller) instanceGone(ctx context.Context, cfg config.Config, instance string) (bool, error) {
	_, err := c.Nodes.CoreV1().Nodes().Get(ctx, "ins-"+strings.TrimPrefix(instance, "ins_"), metav1.GetOptions{})
	if err == nil {
		return false, nil
	}
	if !apierrors.IsNotFound(err) {
		return false, status.Error(codes.Unavailable, "reading the Node: "+err.Error())
	}
	i, _, err := c.Compute.GetInstance(ctx, cfg.Org, cfg.Project, instance)
	if ezghclient.IsNotFound(err) {
		// A force detach is destructive: a 404 means gone only while the project still answers
		// (a wrong project in cloud.json 404s too), as the CCM and lb controllers check.
		if _, perr := c.Compute.ListInstances(ctx, cfg.Org, cfg.Project, computeapi.InstanceFilter{}, 1, ""); perr != nil {
			return false, cloudcsi.APIStatus(perr, nil)
		}
		return true, nil
	}
	if err != nil {
		return false, cloudcsi.APIStatus(err, nil)
	}
	return i.Status == "terminated", nil
}

// waitOperation follows a 202's operation (nil for a 200) for up to attachWait.
func (c *Controller) waitOperation(ctx context.Context, cfg config.Config, op *computeapi.Operation) error {
	deadline := time.Now().Add(attachWait)
	for op != nil {
		switch op.Status {
		case "succeeded":
			return nil
		case "failed", "cancelled":
			return status.Errorf(codes.Internal, "%s %s: %s", op.Type, op.Status, op.ErrorText())
		}
		if err := sleep(ctx, deadline); err != nil {
			return status.Errorf(codes.Aborted, "%s is still %s", op.Type, op.Status)
		}
		next, err := c.Compute.GetOperation(ctx, cfg.Org, op.ID)
		if err != nil {
			return cloudcsi.APIStatus(err, nil)
		}
		op = next
	}
	return nil
}

func (c *Controller) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	vol := req.GetVolumeId()
	if !strings.HasPrefix(vol, "vol_") {
		return nil, status.Errorf(codes.NotFound, "volume %q doesn't exist", vol)
	}
	cfg, err := c.config()
	if err != nil {
		return nil, err
	}
	want, err := sizeFor(req.GetCapacityRange(), 0, 1, 0)
	if err != nil {
		return nil, err
	}
	v, err := c.Blockstore.GetVolume(ctx, cfg.Org, cfg.Project, vol)
	if err != nil {
		return nil, cloudcsi.APIStatus(err, nil)
	}
	if bytes(v.SizeBytes) < want && v.Modification == nil {
		if _, err := c.Blockstore.ModifyVolume(ctx, cfg.Org, cfg.Project, vol, blockstoreapi.VolumeModify{SizeBytes: strconv.FormatInt(want, 10)}); err != nil {
			return nil, cloudcsi.APIStatus(err, nil)
		}
	}
	// Online: the guest sees the larger disk once the growth applies; the node then grows the
	// file system.
	v, err = c.waitVolume(ctx, cfg, vol, attachWait, func(v *blockstoreapi.Volume) bool {
		return v.Modification == nil && bytes(v.SizeBytes) >= want
	})
	if err != nil {
		return nil, err
	}
	if v.Status == "failed" {
		return nil, status.Errorf(codes.FailedPrecondition, "volume %s failed", vol)
	}
	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes: bytes(v.SizeBytes), NodeExpansionRequired: req.GetVolumeCapability().GetBlock() == nil,
	}, nil
}

func (c *Controller) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	cfg, err := c.config()
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(req.GetVolumeId(), "vol_") {
		return nil, status.Errorf(codes.NotFound, "volume %q doesn't exist", req.GetVolumeId())
	}
	if _, err := c.Blockstore.GetVolume(ctx, cfg.Org, cfg.Project, req.GetVolumeId()); err != nil {
		return nil, cloudcsi.APIStatus(err, nil)
	}
	if err := checkCapabilities(req.GetVolumeCapabilities()); err != nil {
		return &csi.ValidateVolumeCapabilitiesResponse{Message: status.Convert(err).Message()}, nil
	}
	return &csi.ValidateVolumeCapabilitiesResponse{Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
		VolumeCapabilities: req.GetVolumeCapabilities(), VolumeContext: req.GetVolumeContext(), Parameters: req.GetParameters(),
	}}, nil
}

func (c *Controller) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	if req.GetName() == "" || req.GetSourceVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "name and source_volume_id are required")
	}
	if !strings.HasPrefix(req.GetSourceVolumeId(), "vol_") {
		return nil, status.Errorf(codes.NotFound, "volume %q doesn't exist", req.GetSourceVolumeId())
	}
	cfg, err := c.config()
	if err != nil {
		return nil, err
	}
	op, err := c.Blockstore.CreateSnapshot(ezghclient.WithIdempotencyKey(ctx, "csi-"+req.GetName()), cfg.Org, cfg.Project, blockstoreapi.SnapshotCreate{
		VolumeID: req.GetSourceVolumeId(), Name: req.GetName(), Tags: cloudcsi.Tags(cfg, req.GetParameters()),
	})
	if err != nil {
		return nil, cloudcsi.APIStatus(err, createConflicts)
	}
	// A retry replays the first answer: read the snapshot as it is now.
	s, err := c.Blockstore.GetSnapshot(ctx, cfg.Org, cfg.Project, op.Snapshot.Slug)
	if err != nil {
		return nil, cloudcsi.APIStatus(err, nil)
	}
	if s.Status == "failed" {
		return nil, status.Errorf(codes.Internal, "snapshot %s failed", s.Slug)
	}
	created, _ := time.Parse(time.RFC3339Nano, s.CreatedAt)
	return &csi.CreateSnapshotResponse{Snapshot: &csi.Snapshot{
		SnapshotId: s.Slug, SourceVolumeId: s.Volume, SizeBytes: bytes(s.VolumeSizeBytes),
		CreationTime: timestamppb.New(created), ReadyToUse: s.Status == "available",
	}}, nil
}

func (c *Controller) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	if !strings.HasPrefix(req.GetSnapshotId(), "snp_") {
		return &csi.DeleteSnapshotResponse{}, nil
	}
	cfg, err := c.config()
	if err != nil {
		return nil, err
	}
	// Only this cluster's snapshot, as DeleteVolume.
	sn, err := c.Blockstore.GetSnapshot(ctx, cfg.Org, cfg.Project, req.GetSnapshotId())
	if ezghclient.IsNotFound(err) {
		return &csi.DeleteSnapshotResponse{}, nil
	}
	if err != nil {
		return nil, cloudcsi.APIStatus(err, nil)
	}
	if !cloudcsi.Owned(cfg, sn.Tags) {
		return &csi.DeleteSnapshotResponse{}, nil
	}
	if _, err := c.Blockstore.DeleteSnapshot(ctx, cfg.Org, cfg.Project, req.GetSnapshotId()); err != nil && !ezghclient.IsNotFound(err) {
		return nil, cloudcsi.APIStatus(err, nil)
	}
	return &csi.DeleteSnapshotResponse{}, nil
}

var accessModes = map[csi.VolumeCapability_AccessMode_Mode]bool{
	csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER:        true,
	csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY:   true,
	csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER: true,
	csi.VolumeCapability_AccessMode_SINGLE_NODE_MULTI_WRITER:  true,
}

// checkCapabilities accepts single-node access, as a block device or an ext4 or xfs mount.
func checkCapabilities(caps []*csi.VolumeCapability) error {
	if len(caps) == 0 || caps[0] == nil {
		return status.Error(codes.InvalidArgument, "volume capabilities are required")
	}
	for _, c := range caps {
		if !accessModes[c.GetAccessMode().GetMode()] {
			return status.Errorf(codes.InvalidArgument, "access mode %s isn't supported: a volume attaches to one node", c.GetAccessMode().GetMode())
		}
		switch {
		case c.GetBlock() != nil:
		case c.GetMount() != nil:
			if err := checkFstype(c.GetMount().GetFsType()); err != nil {
				return err
			}
		default:
			return status.Error(codes.InvalidArgument, "a volume capability is a block device or a mount")
		}
	}
	return nil
}

func checkFstype(fs string) error {
	switch fs {
	case "", "ext4", "xfs":
		return nil
	}
	return status.Errorf(codes.InvalidArgument, "file system %q isn't supported (ext4, xfs)", fs)
}
