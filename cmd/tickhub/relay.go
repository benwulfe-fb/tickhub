package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/benwulfe-fb/tickhub/pkg/relay"
)

func runRelayServer(args []string) {
	fs := flag.NewFlagSet("relay-server", flag.ExitOnError)
	shmName := fs.String("shm", "tickhub_live", "Source SHM segment name to stream from")
	addr := fs.String("addr", "0.0.0.0:8765", "TCP address to bind and listen on")
	fromStart := fs.Bool("from-start", false, "Stream from first anchor committed instead of latest")
	fs.Parse(args)

	cfg := relay.ServerConfig{
		SHMName:      *shmName,
		ListenAddr:   *addr,
		FromStart:    *fromStart,
		PollInterval: 50 * time.Microsecond,
	}

	srv, err := relay.NewServer(cfg)
	if err != nil {
		log.Fatalf("[RELAY-SRV] Failed to create server: %v", err)
	}
	defer srv.Close()

	if err := srv.Start(); err != nil {
		log.Fatalf("[RELAY-SRV] Failed to start server: %v", err)
	}

	log.Printf("[RELAY-SRV] Listening on %s, streaming from /dev/shm/%s (PID: %d)",
		*addr, *shmName, os.Getpid())

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	<-ctx.Done()
	log.Printf("[RELAY-SRV] Shutdown signal received, shutting down relay server...")
}

func runRelayClient(args []string) {
	fs := flag.NewFlagSet("relay-client", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:8765", "Relay server TCP address (e.g. host:8765)")
	shmName := fs.String("shm", "tickhub_live", "Replica SHM segment name to write to")
	noUnlink := fs.Bool("no-unlink", false, "Do not unlink replica SHM segment on exit")
	timeout := fs.Duration("timeout", 10*time.Second, "Connect timeout")
	fs.Parse(args)

	cfg := relay.ClientConfig{
		ServerAddr:   *addr,
		SHMName:      *shmName,
		Permissions:  0666,
		UnlinkOnExit: !*noUnlink,
		Timeout:      *timeout,
	}

	cli := relay.NewClient(cfg)
	defer cli.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log.Printf("[RELAY-CLI] Connecting to %s -> replicating into /dev/shm/%s (PID: %d)",
		*addr, *shmName, os.Getpid())

	if err := cli.ConnectAndReplicate(ctx); err != nil {
		log.Printf("[RELAY-CLI] Replication stopped: %v", err)
	}

	log.Printf("[RELAY-CLI] Exiting. Total replicated anchors: %d", cli.ReplicatedAnchors())
}
