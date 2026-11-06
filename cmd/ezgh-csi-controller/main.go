// Command ezgh-csi-controller serves the CSI controllers of EZGH's drivers on each control-plane
// VM, one subcommand per systemd unit (ezgh-cloud@<subcommand>.service), each on its own unix
// socket under /run/ezgh/csi for that VM's external-provisioner, attacher, resizer and
// snapshotter sidecars. They act on EZGH's public APIs as the cluster's Bot.
package main

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

// subcommands: each file's init adds its own.
var subcommands = map[string]func(args []string) error{}

func main() { os.Exit(dispatch(os.Args[1:], os.Stderr)) }

func dispatch(args []string, stderr io.Writer) int {
	names := make([]string, 0, len(subcommands))
	for name := range subcommands {
		names = append(names, name)
	}
	sort.Strings(names)
	if len(args) == 0 || subcommands[args[0]] == nil {
		fmt.Fprintf(stderr, "usage: ezgh-csi-controller <%s>\n", strings.Join(names, "|"))
		return 2
	}
	if err := subcommands[args[0]](args[1:]); err != nil {
		fmt.Fprintf(stderr, "ezgh-csi-controller %s: %v\n", args[0], err)
		return 1
	}
	return 0
}
