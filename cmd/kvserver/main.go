// Command kvserver runs one node of a raft-kv cluster.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bhattt1/RaftBasedDistributedKey/internal/config"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/httpapi"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/kv"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/node"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/observability"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/security"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/storage"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/transport"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	os.Exit(run())
}

// run wires the node together and blocks until it stops. It returns the
// process exit code: 0 for a requested shutdown, 1 for a failure, 2 for bad
// configuration.
func run() int {
	cfg, err := config.Load(os.Args[1:], os.Getenv, os.Stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		fmt.Fprintln(os.Stderr, "kvserver: configuration error:", err)
		return 2
	}
	log := observability.NewLogger(os.Stderr, cfg.LogLevel, cfg.LogFormat).With("node", cfg.NodeID)
	metrics := observability.NewMetrics()

	if cfg.InsecureDev {
		log.Warn("INSECURE DEVELOPMENT MODE: no TLS between peers, no HTTPS, no client authentication. Do not expose this node to an untrusted network.")
	}
	if cfg.UnsafeNoFsync {
		log.Warn("FSYNC IS DISABLED: acknowledged writes can be lost on a crash or power failure. This mode is for benchmarks only.")
	}
	if len(cfg.Peers)%2 == 0 {
		log.Warn("the cluster has an even number of members; it tolerates no more failures than one member fewer would", "members", len(cfg.Peers))
	}

	// ---- security material -------------------------------------------------
	var (
		peerServerTLS *tls.Config
		peerClientTLS func(string) *tls.Config
		httpTLS       *tls.Config
		token         string
	)
	if !cfg.InsecureDev {
		if peerServerTLS, peerClientTLS, err = security.PeerTLS(cfg.TLS.PeerCA, cfg.TLS.PeerCert, cfg.TLS.PeerKey, cfg.NodeID); err != nil {
			log.Error("loading peer TLS material", "error", err)
			return 2
		}
		if httpTLS, err = security.HTTPServerTLS(cfg.TLS.HTTPCert, cfg.TLS.HTTPKey); err != nil {
			log.Error("loading HTTPS material", "error", err)
			return 2
		}
		if token, err = security.LoadToken(cfg.TLS.ClientTokenFile); err != nil {
			log.Error("loading client token", "error", err)
			return 2
		}
	}

	// ---- storage -----------------------------------------------------------
	// Opening the store takes the directory lock, checks that the directory
	// belongs to this node and cluster, and replays the write-ahead log. Any
	// inconsistency it cannot explain stops the node here, before it can
	// vote or acknowledge anything based on damaged state.
	store, err := storage.Open(storage.Options{
		Dir:              cfg.DataDir,
		ClusterID:        cfg.ClusterID,
		NodeID:           cfg.NodeID,
		SegmentSize:      cfg.WALSegmentBytes,
		MaxSnapshotBytes: kv.DefaultLimits.MaxStateBytes + 64<<20,
		NoSync:           cfg.UnsafeNoFsync,
		OnSync:           metrics.WALSync,
	})
	if err != nil {
		log.Error("opening data directory", "dir", cfg.DataDir, "error", err)
		return 1
	}
	defer store.Close()

	// ---- listeners ---------------------------------------------------------
	// Bind everything before starting the node, so that a port conflict is
	// reported before the node takes part in the cluster.
	raftLis, err := net.Listen("tcp", cfg.RaftListen)
	if err != nil {
		log.Error("listening for peers", "addr", cfg.RaftListen, "error", err)
		return 1
	}
	httpLis, err := net.Listen("tcp", cfg.HTTPListen)
	if err != nil {
		log.Error("listening for clients", "addr", cfg.HTTPListen, "error", err)
		return 1
	}
	var adminLis net.Listener
	if cfg.AdminListen != "" {
		if adminLis, err = net.Listen("tcp", cfg.AdminListen); err != nil {
			log.Error("listening for admin", "addr", cfg.AdminListen, "error", err)
			return 1
		}
	}

	// ---- transport and node ------------------------------------------------
	tr, err := transport.New(transport.Config{
		ClusterID: cfg.ClusterID,
		ID:        cfg.NodeID,
		Peers:     cfg.RaftAddrs(),
		ServerTLS: peerServerTLS,
		ClientTLS: peerClientTLS,
		// An RPC that takes longer than an election timeout is useless to
		// Raft, so there is no point waiting longer for it.
		RPCTimeout: time.Duration(cfg.ElectionTicks) * cfg.TickInterval.D(),
		Logger:     log,
		Observer:   metrics,
	})
	if err != nil {
		log.Error("creating transport", "error", err)
		return 1
	}
	nd, err := node.Start(node.Config{
		ID:                  cfg.NodeID,
		Peers:               cfg.PeerIDs(),
		TickInterval:        cfg.TickInterval.D(),
		ElectionTicks:       cfg.ElectionTicks,
		HeartbeatTicks:      cfg.HeartbeatTicks,
		MaxPendingProposals: cfg.MaxPendingProposals,
		SnapshotThreshold:   cfg.SnapshotThreshold,
		SnapshotTrailing:    cfg.SnapshotTrailing,
		Logger:              log,
		Observer:            metrics,
	}, store, tr)
	if err != nil {
		tr.Close()
		log.Error("starting node", "error", err)
		return 1
	}
	metrics.RegisterState(nd.Status)

	// Each goroutine started below reports its exit on this channel, and
	// main owns shutting all of them down.
	serveErr := make(chan error, 3)
	go func() { serveErr <- fmt.Errorf("peer server: %w", tr.Serve(raftLis, nd)) }()

	api := httpapi.New(nd, httpapi.Options{
		NodeID:         cfg.NodeID,
		ClusterID:      cfg.ClusterID,
		PeerURLs:       cfg.HTTPAddrs(),
		DefaultTimeout: cfg.RequestTimeout.D(),
		MaxTimeout:     cfg.MaxRequestTimeout.D(),
		Token:          token,
		Logger:         log,
		Observe:        metrics.HTTPRequest,
	})
	httpServer := &http.Server{
		Handler:           api.Handler(),
		TLSConfig:         httpTLS,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		// A response may legitimately take as long as the longest request
		// deadline; allow a little beyond that.
		WriteTimeout:   cfg.MaxRequestTimeout.D() + 10*time.Second,
		IdleTimeout:    60 * time.Second,
		MaxHeaderBytes: 16 << 10,
		ErrorLog:       slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}
	go func() {
		if httpTLS != nil {
			serveErr <- fmt.Errorf("client API: %w", httpServer.ServeTLS(httpLis, "", ""))
		} else {
			serveErr <- fmt.Errorf("client API: %w", httpServer.Serve(httpLis))
		}
	}()
	var adminServer *http.Server
	if adminLis != nil {
		adminServer = &http.Server{
			Handler:           api.AdminHandler(metrics.Registry, cfg.EnablePprof),
			ReadHeaderTimeout: 5 * time.Second,
			ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
		}
		go func() { serveErr <- fmt.Errorf("admin: %w", adminServer.Serve(adminLis)) }()
	}

	log.Info("kvserver started", "version", version, "cluster", cfg.ClusterID,
		"raft", raftLis.Addr().String(), "http", httpLis.Addr().String(), "admin", cfg.AdminListen,
		"members", len(cfg.Peers), "secure", !cfg.InsecureDev, "data_dir", cfg.DataDir)

	// ---- wait for a reason to stop -----------------------------------------
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	exit := 0
	select {
	case sig := <-signals:
		log.Info("shutting down", "signal", sig.String())
	case <-nd.Done():
		// The event loop ended on its own, which only happens when storage
		// failed. Exit non-zero so a supervisor notices.
		log.Error("node stopped after a storage failure", "error", nd.Status().Failed)
		exit = 1
	case err := <-serveErr:
		log.Error("a server stopped unexpectedly", "error", err)
		exit = 1
	}
	signal.Stop(signals)

	// ---- bounded shutdown --------------------------------------------------
	// Order matters. Stopping the node first answers every waiting request
	// ("outcome unknown" for anything already accepted: it may still commit
	// on the other nodes), which lets the HTTP server drain immediately.
	ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout.D())
	defer cancel()
	if err := nd.Stop(ctx); err != nil {
		log.Error("stopping node", "error", err)
		exit = 1
	}
	if err := httpServer.Shutdown(ctx); err != nil {
		log.Warn("client API did not drain in time", "error", err)
		httpServer.Close()
	}
	if adminServer != nil {
		adminServer.Close()
	}
	tr.Close()
	if err := store.Close(); err != nil && exit == 0 {
		log.Error("closing storage", "error", err)
		exit = 1
	}
	log.Info("kvserver stopped", "exit_code", exit)
	return exit
}
