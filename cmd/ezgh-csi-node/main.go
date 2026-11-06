// Command ezgh-csi-node is the CSI node plugin of EZGH's drivers, one subcommand per driver, run
// by the add-on's DaemonSet on every node beside node-driver-registrar.
package main

import (
	"fmt"
	"os"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	"k8s.io/mount-utils"
	"k8s.io/utils/exec"

	"github.com/ezgamehost/ezgh-csi/internal/cloud/csi"
	"github.com/ezgamehost/ezgh-csi/internal/cloud/csi/blockstore"
	"github.com/ezgamehost/ezgh-csi/internal/cloud/csi/files"
	"github.com/ezgamehost/ezgh-csi/internal/kit/logging"
)

func main() {
	run := map[string]func() error{"blockstore": runBlockstore, "files": runFiles}
	if len(os.Args) != 2 || run[os.Args[1]] == nil {
		fmt.Fprintln(os.Stderr, "usage: ezgh-csi-node blockstore|files")
		os.Exit(2)
	}
	if err := run[os.Args[1]](); err != nil {
		fmt.Fprintln(os.Stderr, "ezgh-csi-node:", err)
		os.Exit(1)
	}
}

func runBlockstore() error {
	log := logging.New(os.Stdout, "ezgh-csi-node-blockstore", os.Getenv("APP_ENV") == "local")
	klog.SetSlogLogger(log) // mount-utils logs through klog
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return err
	}
	nodes, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}
	return csi.ServeUnix(log, "/var/lib/kubelet/plugins/"+blockstore.Driver+"/csi.sock", blockstore.Identity{}, &blockstore.Node{
		NodeName: os.Getenv("KUBE_NODE_NAME"),
		Nodes:    nodes,
		Mounter:  mount.NewSafeFormatAndMount(mount.New(""), exec.New()),
		DevDir:   "/dev/disk/by-id",
	})
}

// runFiles needs no Kubernetes API: the controller hands each mount its mount target.
func runFiles() error {
	log := logging.New(os.Stdout, "ezgh-csi-node-files", os.Getenv("APP_ENV") == "local")
	klog.SetSlogLogger(log)
	// Linux's mounter forces unmounts of hard NFS mounts whose server is gone.
	m, ok := mount.New("").(mount.MounterForceUnmounter)
	if !ok {
		return fmt.Errorf("this platform's mounter can't force an unmount")
	}
	return csi.ServeUnix(log, "/var/lib/kubelet/plugins/"+files.Driver+"/csi.sock", files.Identity{}, &files.Node{
		NodeName: os.Getenv("KUBE_NODE_NAME"),
		Mounter:  m,
	})
}
