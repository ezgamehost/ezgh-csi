package main

import (
	"os"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/ezgamehost/ezgh-csi/internal/cloud/config"
	"github.com/ezgamehost/ezgh-csi/internal/cloud/csi"
	"github.com/ezgamehost/ezgh-csi/internal/cloud/csi/blockstore"
	"github.com/ezgamehost/ezgh-csi/internal/cloud/run"
	"github.com/ezgamehost/ezgh-csi/internal/ezghclient"
)

func init() { subcommands["csi-blockstore-controller"] = runCSIBlockstore }

// BlockstoreSocket is where the CSI controller sidecars on this VM reach the driver.
const BlockstoreSocket = "/run/ezgh/csi/blockstore.sock"

// runCSIBlockstore serves the Block Storage CSI controller on every control-plane VM. It isn't
// leader-elected itself: the sidecars calling it are (csi-blockstore-*.service).
func runCSIBlockstore([]string) error {
	const sub = "csi-blockstore-controller"
	log := run.Logger(sub)
	// The same overrides run.Manager reads (local runs).
	getenv := func(name, def string) string {
		if v := os.Getenv(name); v != "" {
			return v
		}
		return def
	}
	rest, err := clientcmd.BuildConfigFromFlags("", getenv("EZGH_CLOUD_KUBECONFIG", run.Kubeconfig))
	if err != nil {
		return err
	}
	rest.UserAgent = "ezgh-cloud-" + sub
	nodes, err := kubernetes.NewForConfig(rest)
	if err != nil {
		return err
	}
	clients := ezghclient.New(&ezghclient.LocalSource{URL: getenv("EZGH_CREDENTIALS_URL", ezghclient.DefaultCredentialsURL)}, "ezgh-cloud-"+sub)
	cfgPath := getenv("EZGH_CLOUD_CONFIG", config.Path)
	return csi.ServeUnix(log, BlockstoreSocket, blockstore.Identity{}, &blockstore.Controller{
		Blockstore: clients.Blockstore, Compute: clients.Compute, Nodes: nodes,
		Config: func() (config.Config, error) { return config.Load(cfgPath) },
	})
}
