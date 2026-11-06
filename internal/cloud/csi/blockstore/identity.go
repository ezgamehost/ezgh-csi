// Package blockstore is the CSI driver blockstore.csi.ezghcloud.com: EZGH Block Storage volumes
// as PersistentVolumes. The controller runs on the control-plane VMs as the cluster's Bot; the
// node plugin runs on every node in the blockstore-csi add-on.
package blockstore

import (
	"context"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Driver is the CSI driver's name.
const Driver = "blockstore.csi.ezghcloud.com"

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
		{Type: &csi.PluginCapability_VolumeExpansion_{VolumeExpansion: &csi.PluginCapability_VolumeExpansion{Type: csi.PluginCapability_VolumeExpansion_ONLINE}}},
	}}, nil
}

func (Identity) Probe(context.Context, *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{Ready: wrapperspb.Bool(true)}, nil
}
