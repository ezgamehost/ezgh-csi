package main

import (
	"os"

	"github.com/ezgamehost/ezgh-csi/internal/cloud/config"
	"github.com/ezgamehost/ezgh-csi/internal/cloud/csi"
	"github.com/ezgamehost/ezgh-csi/internal/cloud/csi/files"
	"github.com/ezgamehost/ezgh-csi/internal/cloud/run"
	"github.com/ezgamehost/ezgh-csi/internal/ezghclient"
)

func init() { subcommands["csi-files-controller"] = runCSIFiles }

// FilesSocket is where the CSI controller sidecars on this VM reach the driver.
const FilesSocket = "/run/ezgh/csi/files.sock"

// runCSIFiles serves the Files CSI controller on every control-plane VM. It isn't leader-elected
// itself: the sidecars calling it are (csi-files-*.service).
func runCSIFiles([]string) error {
	const sub = "csi-files-controller"
	getenv := func(name, def string) string {
		if v := os.Getenv(name); v != "" {
			return v
		}
		return def
	}
	clients := ezghclient.New(&ezghclient.LocalSource{URL: getenv("EZGH_CREDENTIALS_URL", ezghclient.DefaultCredentialsURL)}, "ezgh-cloud-"+sub)
	cfgPath := getenv("EZGH_CLOUD_CONFIG", config.Path)
	return csi.ServeUnix(run.Logger(sub), FilesSocket, files.Identity{}, &files.Controller{
		EZGH:   clients,
		Config: func() (config.Config, error) { return config.Load(cfgPath) },
	})
}
