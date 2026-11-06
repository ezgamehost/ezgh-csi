package csi

import (
	"net/http"
	"os"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ezgamehost/ezgh-csi/internal/cloud/config"
	"github.com/ezgamehost/ezgh-csi/internal/cloud/events"
	"github.com/ezgamehost/ezgh-csi/internal/ezghclient"
	"github.com/ezgamehost/ezgh-csi/internal/ezghclient/apiclient"
)

const (
	// FstypeKey is the StorageClass's file system parameter.
	FstypeKey = "csi.storage.k8s.io/fstype"
	// MetadataPrefix is the sidecars' --extra-create-metadata parameters (pvc/name, pv/name,
	// volumesnapshot/name, …), written as tags under kubernetes.io/created-for/.
	MetadataPrefix = "csi.storage.k8s.io/"
)

// Tags are the cluster's tag and the sidecars' metadata (pvc/namespace, pvc/name, pv/name, …).
func Tags(cfg config.Config, params map[string]string) map[string]string {
	t := cfg.ClusterTags()
	for k, v := range params {
		if strings.HasPrefix(k, MetadataPrefix) && k != FstypeKey {
			t["kubernetes.io/created-for/"+strings.TrimPrefix(k, MetadataPrefix)] = v
		}
	}
	return t
}

// Owned reports whether tags carry this cluster's tag: a PV copied into another cluster (a backup
// restored elsewhere in the project) never deletes what the first cluster made.
func Owned(cfg config.Config, tags map[string]string) bool {
	for k, v := range cfg.ClusterTags() {
		if tags[k] != v {
			return false
		}
	}
	return true
}

// APIStatus is an EZGH API error as a gRPC status, which the sidecars turn into events on the PVC,
// VolumeSnapshot or pod (ProvisioningFailed, FailedAttachVolume, VolumeResizeFailed). conflicts
// maps a 409's error code to its gRPC code.
func APIStatus(err error, conflicts map[string]codes.Code) error {
	_, msg := events.Describe(err)
	if _, ok := ezghclient.AsPermission(err); ok {
		return status.Error(codes.PermissionDenied, msg)
	}
	if _, _, ok := ezghclient.AsQuota(err); ok {
		return status.Error(codes.ResourceExhausted, msg)
	}
	e, ok := apiclient.AsError(err)
	if !ok {
		return status.Error(codes.Unavailable, msg) // credentials or transport
	}
	if code, ok := conflicts[e.Code]; ok {
		return status.Error(code, e.Message)
	}
	switch {
	case e.Status == http.StatusBadRequest:
		return status.Error(codes.InvalidArgument, msg)
	case e.Status == http.StatusNotFound:
		return status.Error(codes.NotFound, msg)
	case e.Status == http.StatusConflict:
		return status.Error(codes.FailedPrecondition, msg)
	case e.Status == http.StatusUnauthorized || e.Status == http.StatusTooManyRequests || e.Status >= 500:
		return status.Error(codes.Unavailable, msg)
	}
	return status.Error(codes.Internal, msg)
}

// EnsureDir makes the directory, and reports whether something is already mounted there.
func EnsureDir(m interface {
	IsLikelyNotMountPoint(string) (bool, error)
}, path string) (bool, error) {
	if err := os.MkdirAll(path, 0o750); err != nil {
		return false, status.Error(codes.Internal, err.Error())
	}
	notMounted, err := m.IsLikelyNotMountPoint(path)
	if err != nil {
		return false, status.Error(codes.Internal, err.Error())
	}
	return !notMounted, nil
}
