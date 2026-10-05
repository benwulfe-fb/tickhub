package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/benwulfe-fb/tickhub/pkg/recorder"
)

func runRecord(args []string) {
	fs := flag.NewFlagSet("record", flag.ExitOnError)
	shmName := fs.String("shm", "tickhub_live", "Source SHM segment name to record from")
	outDir := fs.String("out-dir", "./recordings", "Output root directory for partitioned Parquet recordings")
	outFile := fs.String("file", "", "Optional direct Parquet output file path override")
	symbolsArg := fs.String("symbols", "", "Comma-separated symbol whitelist or path to symbol text file")
	flushInterval := fs.Duration("flush-interval", 10*time.Second, "Buffered row flush interval")
	batchSize := fs.Int("batch-size", 5000, "Maximum buffered rows before immediate flush")
	pollInterval := fs.Duration("poll-interval", 50*time.Millisecond, "Polling interval for anchor advance checks")
	fs.Parse(args)

	var symbols []string
	if *symbolsArg != "" {
		if fi, err := os.Stat(*symbolsArg); err == nil && !fi.IsDir() {
			data, err := os.ReadFile(*symbolsArg)
			if err != nil {
				log.Fatalf("[RECORD] Failed to read symbols file %s: %v", *symbolsArg, err)
			}
			for _, line := range strings.Split(string(data), "\n") {
				line = strings.TrimSpace(line)
				if line != "" && !strings.HasPrefix(line, "#") {
					symbols = append(symbols, line)
				}
			}
			log.Printf("[RECORD] Loaded %d symbols from %s", len(symbols), *symbolsArg)
		} else {
			for _, s := range strings.Split(*symbolsArg, ",") {
				s = strings.TrimSpace(s)
				if s != "" {
					symbols = append(symbols, s)
				}
			}
		}
	}

	cfg := recorder.Config{
		SHMName:       *shmName,
		OutDir:        *outDir,
		OutFile:       *outFile,
		Symbols:       symbols,
		FlushInterval: *flushInterval,
		BatchSize:     *batchSize,
		PollInterval:  *pollInterval,
	}

	rec, err := recorder.NewRecorder(cfg)
	if err != nil {
		log.Fatalf("[RECORD] Failed to initialize recorder: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log.Printf("[RECORD] Starting 1Hz telemetry recorder on /dev/shm/%s (OutDir: %s, Flush: %v, Batch: %d, PID: %d)",
		*shmName, *outDir, *flushInterval, *batchSize, os.Getpid())

	// Periodic telemetry reporting goroutine
	go func() {
		reportTicker := time.NewTicker(30 * time.Second)
		defer reportTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-reportTicker.C:
				st := rec.Stats()
				log.Printf("[RECORD] Telemetry: Flushed %d rows | Files: %d | Anchor: %d | Lag: %.2f ms | Dropped: %d",
					st.FlushedRows, st.FilesWritten, st.LastAnchorNS, float64(st.AnchorLagNS)/1e6, st.DroppedFrames)
			}
		}
	}()

	errCh := make(chan error, 1)
	go func() {
		errCh <- rec.Start(ctx)
	}()

	select {
	case <-ctx.Done():
		log.Printf("[RECORD] Shutdown signal received; performing bounded flush...")
	case err := <-errCh:
		if err != nil {
			log.Printf("[RECORD] Recorder stopped with error: %v", err)
		}
	}

	// Bounded shutdown flush (max 5s)
	done := make(chan struct{})
	go func() {
		if err := rec.Close(); err != nil {
			log.Printf("[RECORD] Close error: %v", err)
		}
		close(done)
	}()

	select {
	case <-done:
		st := rec.Stats()
		log.Printf("[RECORD] Clean shutdown complete. Total rows flushed: %d in %d files.",
			st.FlushedRows, st.FilesWritten)
	case <-time.After(5 * time.Second):
		log.Printf("[RECORD] WARNING: Shutdown flush timed out after 5s.")
	}
}
