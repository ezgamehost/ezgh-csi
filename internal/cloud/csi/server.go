// Package csi serves EZGH's CSI drivers over a unix socket: the controllers on each control-plane
// VM (behind the kubernetes-csi sidecars, which hold the leader election) and the node plugins.
package csi

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// ServeUnix serves servers (any of csi.IdentityServer, ControllerServer, NodeServer) on the unix
// socket at path until SIGTERM or SIGINT, then drains: running calls finish before it returns.
// Failed calls are logged with their method and gRPC code, never their request.
func ServeUnix(log *slog.Logger, path string, servers ...any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	lis, err := net.Listen("unix", path)
	if err != nil {
		return err
	}
	srv := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		resp, err := h(ctx, req)
		if err != nil {
			s := status.Convert(err)
			log.Warn("CSI call failed", "method", info.FullMethod, "code", s.Code().String(), "error", s.Message())
		}
		return resp, err
	}))
	for _, s := range servers {
		if v, ok := s.(csi.IdentityServer); ok {
			csi.RegisterIdentityServer(srv, v)
		}
		if v, ok := s.(csi.ControllerServer); ok {
			csi.RegisterControllerServer(srv, v)
		}
		if v, ok := s.(csi.NodeServer); ok {
			csi.RegisterNodeServer(srv, v)
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()
	log.Info("serving CSI", "socket", path)
	return srv.Serve(lis)
}
