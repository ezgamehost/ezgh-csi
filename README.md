# ezgh-csi

The CSI drivers for EZGH Kubernetes, the counterpart of aws-ebs-csi-driver and aws-efs-csi-driver:

| Driver | Storage |
| --- | --- |
| `blockstore.csi.ezghcloud.com` | EZGH Block Storage volumes and snapshots (dynamic provisioning, attach, expand, snapshots) |
| `files.csi.ezghcloud.com` | EZGH Files file systems over NFS 4.1 (an access point per dynamic volume, or a static `fsy_` volume) |

Two binaries:

| Binary | Runs | How |
| --- | --- | --- |
| `ezgh-csi-node` | `blockstore` or `files`: the node plugin, on every node | the `ezgh-csi-node` image, in the blockstore-csi and files-csi add-ons' DaemonSets beside node-driver-registrar |
| `ezgh-csi-controller` | `csi-blockstore-controller` on `/run/ezgh/csi/blockstore.sock`, `csi-files-controller` on `/run/ezgh/csi/files.sock`: the controller plugins, on each control-plane VM | systemd units `ezgh-cloud@csi-blockstore-controller` and `ezgh-cloud@csi-files-controller`, called by the upstream external-provisioner, attacher, resizer and snapshotter |

The controllers act on EZGH's public APIs as the cluster's Bot, with credentials from the local agent
(`http://127.0.0.1:9444/v1/cluster-credentials`), and read `/etc/ezgh/cloud.json` and
`/etc/ezgh/kubeconfig/ezgh-cloud.conf`; `EZGH_CLOUD_CONFIG`, `EZGH_CLOUD_KUBECONFIG` and
`EZGH_CREDENTIALS_URL` override them for local runs. They delete only volumes and access points
tagged with their own cluster. Logs are JSON lines through the vendored kit (`internal/kit/logging`).

## Build and test

Go 1.27.1 (`go.mod`).

| Check | Command |
| --- | --- |
| Build | `CGO_ENABLED=0 GOOS=linux go build ./cmd/...` |
| Image | `docker build .` (Ubuntu with e2fsprogs, xfsprogs, util-linux and nfs-common: the node plugin formats, mounts and grows file systems) |
| Tests | `go test -race $(go list ./... \| grep -v /internal/kit/)` |
| Lint | `gofmt -l .`; `go vet` and `go run honnef.co/go/tools/cmd/staticcheck@v0.8.1` over `$(go list ./... \| grep -v /internal/kit/)` (service-template checks the kit) |
| Logging | `scripts/check-logging.sh` |

CI (`.github/workflows/ci.yml`) runs them and builds the image, without pushing it, on every pull
request and push to main.

## Releases

Push a tag `vX.Y.Z` on main. `.github/workflows/release.yml` publishes the GitHub release
(`ezgh-csi-node-linux-{amd64,arm64}`, `ezgh-csi-controller-linux-{amd64,arm64}`, `SHA256SUMS`) and
pushes the linux/amd64 image to the kubernetes-addons Registry as
`<host>/kubernetes/ezgh-csi-node:<tag>`. The image goes first and the binaries only after it, from GitHub Environment `release`
(limited to tags `v*`): secret `KUBERNETES_ADDONS_REGISTRY_KEY` (a platform-organization Bot key that can push to project
kubernetes-addons) and variable `KUBERNETES_ADDONS_REGISTRY_HOST` (`<hostId>.registry.ezghcloud.com`).
A published tag never moves: a fix ships as a new version. The add-on bundle runs the image's
commands and flags as they are: changing one needs a new bundle version in kubernetes-stack too.

kubernetes-stack consumes both: the `blockstore-csi` and `files-csi` add-on bundles pin the image in
their `images.json` (`ezgh-csi-node`), and the control-plane payload pins `ezgh-csi-controller` by
version and SHA-256 in `deploy/images/releases.txt`. The systemd units (a drop-in points
`ezgh-cloud@csi-*-controller` at this binary) and the sidecars live in kubernetes-stack.

## Copied code

Other repositories' code is copied in, not imported, each copy with its own sync:

| Path | Source | Sync |
| --- | --- | --- |
| `internal/kit` | ezgamehost/service-template `kit/go` (commit in `internal/kit/KIT_VERSION`) | `scripts/sync-kit.sh` |
| `internal/ezghclient`, `internal/cloud/{ccm,config,events,run}` | ezgamehost/ezgh-cloud-controller (commit in `COPIES`); its `internal/ezghclient` is itself ezgh-cli's API clients plus its own additions | `scripts/sync-copies.sh ezgh-cloud-controller` |

## License

Apache-2.0 ([LICENSE](LICENSE)). Security reports: [SECURITY.md](SECURITY.md).
