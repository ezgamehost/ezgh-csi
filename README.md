# ezgh-csi

[Container Storage Interface](https://kubernetes-csi.github.io/docs/) drivers for
[EZGH Kubernetes](https://docs.ezghcloud.com), backed by EZGH Block Storage and EZGH Files.

EZGH Kubernetes installs and upgrades these drivers as the `blockstore-csi` and `files-csi`
add-ons. This repository holds their source code; you don't need to deploy them yourself.

## Drivers

| Driver | Backed by | Capabilities |
| --- | --- | --- |
| `blockstore.csi.ezghcloud.com` | EZGH Block Storage | Dynamic provisioning, live attach and detach, online expansion, snapshots and restore (`ReadWriteOnce`) |
| `files.csi.ezghcloud.com` | EZGH Files | Shared file systems over NFS 4.1 with optional TLS, an access point per dynamically provisioned volume, or static volumes on an existing file system (`ReadWriteMany`) |

The drivers only modify or delete volumes and access points tagged as belonging to their own
cluster.

## Components

| Binary | Runs on | Purpose |
| --- | --- | --- |
| `ezgh-csi-node` | Every node, as a DaemonSet | Node plugin: formats, mounts, unmounts and grows volumes |
| `ezgh-csi-controller` | The cluster's control plane | Controller plugin: creates, attaches, expands, snapshots and deletes volumes through the EZGH APIs, called by the standard CSI sidecars |

The controller acts as the cluster's Bot with short-lived credentials from the control plane's
local agent. It reads its configuration from `/etc/ezgh/cloud.json` and
`/etc/ezgh/kubeconfig/ezgh-cloud.conf`; set `EZGH_CLOUD_CONFIG`, `EZGH_CLOUD_KUBECONFIG` and
`EZGH_CREDENTIALS_URL` to override them for local development. Logs are structured JSON.

## Container image and binaries

| Artifact | Location |
| --- | --- |
| Node plugin image (linux/amd64) | `q5zljyjh52il.registry.ezghcloud.com/kubernetes/ezgh-csi-node:<version>` |
| Binaries (linux/amd64, linux/arm64) with `SHA256SUMS` | [GitHub Releases](https://github.com/ezgamehost/ezgh-csi/releases) |

## Building from source

Requires Go 1.27.1 or later.

```sh
CGO_ENABLED=0 GOOS=linux go build ./cmd/...
docker build .
```

## Testing

| Check | Command |
| --- | --- |
| Unit tests | `go test -race $(go list ./... \| grep -v /internal/kit/)` |
| Lint | `gofmt -l .`, `go vet` and `staticcheck` |
| Logging | `scripts/check-logging.sh` |

Continuous integration runs these and builds the image on every pull request and push to `main`.

## Releases

Releases follow semantic versioning. Pushing a `vX.Y.Z` tag on `main` publishes the container image
and then the binaries. Published versions are immutable; fixes ship as new versions.

## Vendored code

`internal/kit`, `internal/ezghclient` and parts of `internal/cloud` are vendored from shared EZGH
libraries and refreshed with `scripts/sync-kit.sh` and `scripts/sync-copies.sh`. Changes to them
are made upstream.

## Security

Please report vulnerabilities privately as described in [SECURITY.md](SECURITY.md).

## License

Licensed under the [Apache License 2.0](LICENSE).
