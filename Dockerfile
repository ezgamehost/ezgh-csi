# Build context: the repository root. The CSI node plugin of the blockstore-csi and files-csi
# add-ons: it formats, mounts and grows file systems on the node, so it carries their tools.
# Tagged releases push it to the kubernetes-addons Registry as kubernetes/ezgh-csi-node:<tag>.
FROM mirror.gcr.io/library/golang:1.27.1-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ezgh-csi-node ./cmd/ezgh-csi-node

FROM mirror.gcr.io/library/ubuntu:26.04
RUN apt-get update \
  && apt-get install -y --no-install-recommends e2fsprogs xfsprogs util-linux nfs-common \
  && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/ezgh-csi-node /usr/bin/ezgh-csi-node
ENTRYPOINT ["/usr/bin/ezgh-csi-node"]
