// Package files is the CSI driver files.csi.ezghcloud.com: EZGH Files file systems as
// ReadWriteMany PersistentVolumes. Dynamic volumes are access points of a file system the
// StorageClass names; static volumes name a file system (or a folder of one) or an access point.
// The controller runs on the control-plane VMs as the cluster's Bot; the node plugin runs on
// every node in the files-csi add-on.
package files

import (
	"context"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/ezgamehost/ezgh-csi/internal/cloud/ccm"
	"github.com/ezgamehost/ezgh-csi/internal/cloud/config"
	cloudcsi "github.com/ezgamehost/ezgh-csi/internal/cloud/csi"
	"github.com/ezgamehost/ezgh-csi/internal/cloud/events"
	"github.com/ezgamehost/ezgh-csi/internal/ezghclient"
	"github.com/ezgamehost/ezgh-csi/internal/ezghclient/filesapi"
)

// Driver is the CSI driver's name.
const Driver = "files.csi.ezghcloud.com"

// Version is the driver's version, the add-on bundle's.
const Version = "0.1.0"

// Identity is the CSI Identity service, the same on the controller and the node.
type Identity struct {
	csi.UnimplementedIdentityServer
}

func (Identity) GetPluginInfo(context.Context, *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{Name: Driver, VendorVersion: Version}, nil
}

func (Identity) GetPluginCapabilities(context.Context, *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{Capabilities: []*csi.PluginCapability{
		{Type: &csi.PluginCapability_Service_{Service: &csi.PluginCapability_Service{Type: csi.PluginCapability_Service_CONTROLLER_SERVICE}}},
	}}, nil
}

func (Identity) Probe(context.Context, *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{Ready: wrapperspb.Bool(true)}, nil
}

// Controller is the CSI Controller service. The kubernetes-csi sidecars on the same VM call it,
// and hold the leader election; it acts on EZGH's APIs as the cluster's Bot. Nothing is cached.
type Controller struct {
	csi.UnimplementedControllerServer
	EZGH *ezghclient.Clients
	// Config reads cloud.json on each call.
	Config func() (config.Config, error)
}

// How long CreateVolume follows a new access point before answering Aborted for the provisioner
// to retry. The provisioner's --timeout is longer.
var (
	pollEvery  = 2 * time.Second
	createWait = 30 * time.Second
)

// createLinks is how many access points one name may go through when they fail.
const createLinks = 3

var (
	idRe    = regexp.MustCompile(`^(fsy|fap)_[a-z0-9]{12}$`)
	permsRe = regexp.MustCompile(`^0?[0-7]{3}$`)
)

func (c *Controller) ControllerGetCapabilities(context.Context, *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	out := &csi.ControllerGetCapabilitiesResponse{}
	for _, t := range []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME, csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
	} {
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

// CreateVolume makes access point pvc-<uid> exporting /kubernetes/<pv name> of the StorageClass's
// file system, owned by and squashed to uid:gid. The file system is elastic: the requested
// capacity is echoed back.
func (c *Controller) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	if err := checkCapabilities(req.GetVolumeCapabilities()); err != nil {
		return nil, err
	}
	if req.GetVolumeContentSource() != nil {
		return nil, status.Error(codes.InvalidArgument, "Files volumes can't be cloned or restored from a snapshot")
	}
	var fs, tls string
	uid, gid, perms := uint32(1000), uint32(1000), "0770"
	for k, v := range req.GetParameters() {
		switch k {
		case "fileSystem":
			fs = v
		case "tls":
			if v != "true" && v != "false" {
				return nil, status.Error(codes.InvalidArgument, `parameter tls is "true" or "false"`)
			}
			tls = v
		case "uid", "gid":
			n, err := strconv.ParseUint(v, 10, 32)
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "parameter %s must be a user or group ID (0-4294967295)", k)
			}
			if k == "uid" {
				uid = uint32(n)
			} else {
				gid = uint32(n)
			}
		case "directoryPerms":
			if !permsRe.MatchString(v) {
				return nil, status.Error(codes.InvalidArgument, "parameter directoryPerms must be an octal mode such as 0770")
			}
			perms = v
		default:
			if !strings.HasPrefix(k, cloudcsi.MetadataPrefix) {
				return nil, status.Errorf(codes.InvalidArgument, "unknown parameter %q (fileSystem, tls, uid, gid, directoryPerms)", k)
			}
		}
	}
	if !strings.HasPrefix(fs, "fsy_") || !idRe.MatchString(fs) {
		return nil, status.Error(codes.InvalidArgument, "parameter fileSystem (fsy_…) is required: copy StorageClass ezgh-files and name your file system")
	}
	cfg, err := c.config()
	if err != nil {
		return nil, err
	}
	ap, err := c.createAccessPoint(ctx, cfg, req.GetName(), filesapi.AccessPointCreate{
		FileSystem: fs, Name: req.GetName(), Tags: cloudcsi.Tags(cfg, req.GetParameters()),
		RootDirectory: &filesapi.RootDirectoryInput{
			Path:         "/kubernetes/" + req.GetName(),
			CreationInfo: &filesapi.CreationInfo{OwnerUID: uid, OwnerGID: gid, Permissions: perms},
		},
		PosixUser: &filesapi.PosixUserInput{UID: uid, GID: gid},
	})
	if err != nil {
		return nil, err
	}
	var vc map[string]string
	if tls == "true" {
		vc = map[string]string{"tls": "true"}
	}
	return &csi.CreateVolumeResponse{Volume: &csi.Volume{
		VolumeId: ap.Slug, CapacityBytes: req.GetCapacityRange().GetRequiredBytes(), VolumeContext: vc,
	}}, nil
}

// createAccessPoint creates the access point once per name: the CO retries under the same name
// (pvc-<uid>), and the Idempotency-Key csi-<name> gets the same one back. One that failed (or is
// being deleted, or is gone) is deleted and replaced under csi-<name>-<its fap_>, so every retry
// walks the same chain and none creates a second access point.
func (c *Controller) createAccessPoint(ctx context.Context, cfg config.Config, name string, body filesapi.AccessPointCreate) (*filesapi.AccessPoint, error) {
	key := "csi-" + name
	for link := 1; ; link++ {
		op, err := c.EZGH.Files.CreateAccessPoint(ezghclient.WithIdempotencyKey(ctx, key), cfg.Org, cfg.Project, body)
		if err != nil {
			return nil, cloudcsi.APIStatus(err, map[string]codes.Code{
				"idempotency_conflict": codes.AlreadyExists, "idempotency_in_progress": codes.Aborted,
			})
		}
		ap, err := c.waitAccessPoint(ctx, cfg, op.Target.ID)
		gone := status.Code(err) == codes.NotFound
		if err != nil && !gone {
			return nil, err
		}
		if !gone && ap.Status != "failed" && ap.Status != "deleting" {
			return ap, nil
		}
		id := op.Target.ID
		if !gone {
			id = ap.Slug
		}
		if !gone && ap.Status == "failed" {
			_, _ = c.EZGH.Files.DeleteAccessPoint(ctx, cfg.Org, cfg.Project, id) // best effort; it's tagged with the cluster
		}
		if link == createLinks {
			reason := "it no longer exists"
			if !gone && ap.Failure != nil {
				reason = ap.Failure.Message
			}
			return nil, status.Errorf(codes.Internal, "access point %s failed (%s), as did the %d before it", id, reason, createLinks-1)
		}
		// The operation's target ID, whichever branch: the failed one read as failed on one try
		// and as gone on the next must give the same key, or a retry orphans a link.
		key = "csi-" + name + "-" + op.Target.ID
	}
}

// waitAccessPoint reads the access point (by the operation's target ID) until it's no longer
// creating, for up to createWait.
func (c *Controller) waitAccessPoint(ctx context.Context, cfg config.Config, id string) (*filesapi.AccessPoint, error) {
	deadline := time.Now().Add(createWait)
	for {
		ap, err := c.EZGH.Files.GetAccessPoint(ctx, cfg.Org, cfg.Project, id)
		if err != nil {
			return nil, cloudcsi.APIStatus(err, nil)
		}
		if ap.Status != "creating" {
			return ap, nil
		}
		if time.Now().Add(pollEvery).After(deadline) {
			return nil, status.Errorf(codes.Aborted, "access point %s is still creating", ap.Slug)
		}
		select {
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		case <-time.After(pollEvery):
		}
	}
}

// DeleteVolume deletes a dynamic volume's access point, only when it carries this cluster's tag;
// the files under it stay in the file system. A static volume's file system is never touched.
func (c *Controller) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	h, err := parseHandle(req.GetVolumeId())
	if err != nil || h.accessPoint == "" {
		return &csi.DeleteVolumeResponse{}, nil
	}
	cfg, err := c.config()
	if err != nil {
		return nil, err
	}
	ap, err := c.EZGH.Files.GetAccessPoint(ctx, cfg.Org, cfg.Project, h.accessPoint)
	if ezghclient.IsNotFound(err) {
		return &csi.DeleteVolumeResponse{}, nil
	}
	if err != nil {
		return nil, cloudcsi.APIStatus(err, nil)
	}
	if !cloudcsi.Owned(cfg, ap.Tags) {
		return &csi.DeleteVolumeResponse{}, nil // another cluster's (or the customer's) access point
	}
	if _, err := c.EZGH.Files.DeleteAccessPoint(ctx, cfg.Org, cfg.Project, h.accessPoint); err != nil && !ezghclient.IsNotFound(err) {
		return nil, cloudcsi.APIStatus(err, map[string]codes.Code{"access_point_busy": codes.Aborted})
	}
	return &csi.DeleteVolumeResponse{}, nil
}

// ControllerPublishVolume attaches nothing: it picks the file system's mount target for the node,
// read live, and hands its hostname to the node in publish_context.
func (c *Controller) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	h, err := parseHandle(req.GetVolumeId())
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(req.GetNodeId(), "ins_") {
		return nil, status.Errorf(codes.NotFound, "node %q doesn't exist", req.GetNodeId())
	}
	if err := checkCapabilities([]*csi.VolumeCapability{req.GetVolumeCapability()}); err != nil {
		return nil, err
	}
	if _, err := useTLS(req.GetVolumeContext()); err != nil {
		return nil, err
	}
	cfg, err := c.config()
	if err != nil {
		return nil, err
	}
	fs := h.fileSystem
	if h.accessPoint != "" {
		ap, err := c.EZGH.Files.GetAccessPoint(ctx, cfg.Org, cfg.Project, h.accessPoint)
		if err != nil {
			return nil, cloudcsi.APIStatus(err, nil)
		}
		if ap.Status != "available" {
			return nil, status.Errorf(codes.FailedPrecondition, "access point %s is %s", h.accessPoint, ap.Status)
		}
		fs = ap.FileSystem
	}
	var available []filesapi.MountTarget
	cursor := ""
	for {
		page, err := c.EZGH.Files.ListMountTargets(ctx, cfg.Org, cfg.Project, fs, 100, cursor)
		if err != nil {
			return nil, cloudcsi.APIStatus(err, nil)
		}
		for _, mt := range page.Items {
			if mt.Status == "available" && mt.Hostname != nil && *mt.Hostname != "" && mt.VPC == cfg.VPC {
				available = append(available, mt)
			}
		}
		if page.NextCursor == nil || *page.NextCursor == "" {
			break
		}
		cursor = *page.NextCursor
	}
	if len(available) == 0 {
		return nil, status.Errorf(codes.FailedPrecondition, "%s: file system %s has no available mount target in the cluster's VPC %s; create one", events.NoMountTarget, fs, cfg.VPC)
	}
	pick := available[0]
	if len(available) > 1 {
		// Files serves every subnet from one mount target; the node's own subnet is a preference.
		subnet, err := c.nodeSubnet(ctx, cfg, req.GetNodeId())
		if err != nil {
			return nil, err
		}
		for _, mt := range available {
			if mt.Subnet == subnet {
				pick = mt
				break
			}
		}
	}
	return &csi.ControllerPublishVolumeResponse{PublishContext: map[string]string{"hostname": *pick.Hostname}}, nil
}

// nodeSubnet is the subnet of the instance's interface in the cluster's VPC.
func (c *Controller) nodeSubnet(ctx context.Context, cfg config.Config, instance string) (string, error) {
	inst, err := ccm.GetInstance(ctx, c.EZGH, cfg, instance)
	if err != nil {
		return "", cloudcsi.APIStatus(err, nil)
	}
	for _, eni := range inst.ENIs {
		ni, err := c.EZGH.Network.GetNetworkInterface(ctx, cfg.Org, cfg.Project, eni)
		if err != nil {
			return "", cloudcsi.APIStatus(err, nil)
		}
		if ni.VPC != nil && *ni.VPC == cfg.VPC && ni.Subnet != nil && ni.Attachment != nil &&
			(ni.Attachment.Instance == inst.Slug || ni.Attachment.Instance == inst.ID) {
			return *ni.Subnet, nil
		}
	}
	return "", nil
}

// ControllerUnpublishVolume has nothing to undo.
func (c *Controller) ControllerUnpublishVolume(context.Context, *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (c *Controller) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	h, err := parseHandle(req.GetVolumeId())
	if err != nil {
		return nil, status.Error(codes.NotFound, status.Convert(err).Message())
	}
	cfg, err := c.config()
	if err != nil {
		return nil, err
	}
	if h.accessPoint != "" {
		_, err = c.EZGH.Files.GetAccessPoint(ctx, cfg.Org, cfg.Project, h.accessPoint)
	} else {
		_, err = c.EZGH.Files.GetFileSystem(ctx, cfg.Org, cfg.Project, h.fileSystem)
	}
	if err != nil {
		return nil, cloudcsi.APIStatus(err, nil)
	}
	if err := checkCapabilities(req.GetVolumeCapabilities()); err != nil {
		return &csi.ValidateVolumeCapabilitiesResponse{Message: status.Convert(err).Message()}, nil
	}
	return &csi.ValidateVolumeCapabilitiesResponse{Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
		VolumeCapabilities: req.GetVolumeCapabilities(), VolumeContext: req.GetVolumeContext(), Parameters: req.GetParameters(),
	}}, nil
}

// useTLS reads volumeAttributes.tls: "true", "false" or unset.
func useTLS(vc map[string]string) (bool, error) {
	switch vc["tls"] {
	case "", "false":
		return false, nil
	case "true":
		return true, nil
	}
	return false, status.Errorf(codes.InvalidArgument, `volume attribute tls is "true" or "false", not %q`, vc["tls"])
}

// handle is a volume ID: an access point (dynamic, or static fap_…), or a file system with an
// optional folder (static fsy_… or fsy_…:/sub/path).
type handle struct{ fileSystem, accessPoint, path string }

// parseHandle refuses anything else, and any folder that isn't absolute and normalized (no ..).
func parseHandle(id string) (handle, error) {
	id, folder, hasFolder := strings.Cut(id, ":")
	if !idRe.MatchString(id) {
		return handle{}, status.Errorf(codes.InvalidArgument, "volume handle %q isn't fsy_…, fsy_…:/folder or fap_…", id)
	}
	if strings.HasPrefix(id, "fap_") {
		if hasFolder {
			return handle{}, status.Error(codes.InvalidArgument, "an access point's volume handle takes no folder: it exports its own")
		}
		return handle{accessPoint: id}, nil
	}
	if hasFolder && (!strings.HasPrefix(folder, "/") || path.Clean(folder) != folder) {
		return handle{}, status.Errorf(codes.InvalidArgument, "volume handle folder %q must be an absolute, normalized path with no ..", folder)
	}
	if folder == "/" {
		folder = ""
	}
	return handle{fileSystem: id, path: folder}, nil
}

// checkCapabilities accepts every access mode, mounted (NFS); never a raw block device.
func checkCapabilities(caps []*csi.VolumeCapability) error {
	if len(caps) == 0 || caps[0] == nil {
		return status.Error(codes.InvalidArgument, "volume capabilities are required")
	}
	for _, c := range caps {
		if c.GetMount() == nil {
			return status.Error(codes.InvalidArgument, "Files volumes are file systems: volumeMode Block isn't supported")
		}
		if fs := c.GetMount().GetFsType(); fs != "" && fs != "nfs" && fs != "nfs4" {
			return status.Errorf(codes.InvalidArgument, "file system %q isn't supported: Files volumes are NFS", fs)
		}
		if c.GetAccessMode().GetMode() == csi.VolumeCapability_AccessMode_UNKNOWN {
			return status.Error(codes.InvalidArgument, "an access mode is required")
		}
	}
	return nil
}
