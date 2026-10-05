package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/benwulfe-fb/tickhub/pkg/playback"
)

func runPlayback(args []string) {
	fs := flag.NewFlagSet("playback", flag.ExitOnError)
	filePath := fs.String("file", "", "Path to single Parquet file to replay")
	dirPath := fs.String("dir", "", "Path to directory containing Parquet files to replay")
	shmName := fs.String("shm", "tickhub_playback", "Target SHM segment name to publish replay stream into")
	speed := fs.Float64("speed", 1.0, "Playback speed multiplier (1.0 = real-time, 5.0 = 5x, 0 = unthrottled burst)")
	loop := fs.Bool("loop", false, "Loop playback continuously upon reaching EOF")
	noUnlink := fs.Bool("no-unlink", false, "Do not unlink SHM segment on shutdown")
	timeout := fs.Duration("timeout", 10*time.Second, "Flow control timeout waiting for consumer advance")
	fs.Parse(args)

	source := *filePath
	if source == "" {
		source = *dirPath
	}
	if source == "" {
		log.Fatalf("[PLAYBACK] Must specify either -file <path> or -dir <path>")
	}

	cfg := playback.Config{
		SHMName:            *shmName,
		SourcePath:         source,
		Speed:              *speed,
		Loop:               *loop,
		UnlinkOnExit:       !*noUnlink,
		FlowControlTimeout: *timeout,
	}

	eng, err := playback.NewEngine(cfg)
	if err != nil {
		log.Fatalf("[PLAYBACK] Failed to initialize playback engine: %v", err)
	}
	defer eng.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log.Printf("[PLAYBACK] Starting historical 1Hz telemetry replay -> /dev/shm/%s (Source: %s, Speed: %.1fx, Loop: %v, PID: %d)",
		*shmName, source, *speed, *loop, os.Getpid())

	// Periodic telemetry reporting
	go func() {
		reportTicker := time.NewTicker(15 * time.Second)
		defer reportTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-reportTicker.C:
				st := eng.Stats()
				log.Printf("[PLAYBACK] Status: Replayed %d anchors (%d rows) | FlowControl wait: %.2f ms",
					st.AnchorsReplayed, st.TotalRows, float64(st.FlowControlWaitNS)/1e6)
			}
		}
	}()

	start := time.Now()
	if err := eng.Run(ctx); err != nil && ctx.Err() == nil {
		log.Printf("[PLAYBACK] Playback terminated with error: %v", err)
	}

	elapsed := time.Since(start)
	st := eng.Stats()
	rate := 0.0
	if elapsed.Seconds() > 0 {
		rate = float64(st.AnchorsReplayed) / elapsed.Seconds()
	}

	log.Printf("[PLAYBACK] Playback finished. Replayed %d anchors (%d rows) in %v (%.1f anchors/s).",
		st.AnchorsReplayed, st.TotalRows, elapsed.Round(time.Millisecond), rate)
}
