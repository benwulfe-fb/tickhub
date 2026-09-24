package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/benwulfe-fb/tickhub/pkg/shm"
)

func runStatus(args []string) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	shmName := fs.String("shm", "tickhub_live", "Shared memory segment name (without /dev/shm/)")
	watch := fs.Bool("watch", false, "Continuously refresh status dashboard every 250ms")
	fs.Parse(args)

	rawName := *shmName
	cleanName := strings.TrimPrefix(rawName, "/dev/shm/")
	cleanName = strings.TrimPrefix(cleanName, "tickhub_")

	seg, err := shm.AttachSegment(cleanName, true)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: cannot attach to /dev/shm/tickhub_%s: %v\n", cleanName, err)
		os.Exit(1)
	}
	defer seg.Close(false)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	for {
		printDashboard(cleanName, seg)
		if !*watch {
			break
		}
		select {
		case <-ctx.Done():
			fmt.Println("\nExiting status monitor.")
			return
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func printDashboard(name string, seg *shm.Segment) {
	hdr := seg.Header()
	status := atomic.LoadUint32(&hdr.Status)
	gen := atomic.LoadUint32(&hdr.Generation)
	recMode := atomic.LoadUint32(&hdr.RecoveryMode)
	bootID := atomic.LoadUint64(&hdr.BootID)
	pid := atomic.LoadInt64(&hdr.DaemonPID)
	hb := atomic.LoadInt64(&hdr.HeartbeatNS)
	pubLatNS := atomic.LoadInt64(&hdr.AnchorPublishLatencyNS)
	watermarkNS := atomic.LoadInt64(&hdr.WatermarkBufferNS)
	lastAnchorNS := atomic.LoadInt64(&hdr.LastWrittenAnchorNS)
	dropped := atomic.LoadUint64(&hdr.DroppedTickCount)
	total := atomic.LoadUint64(&hdr.TotalTickCount)

	now := time.Now().UnixNano()
	var hbAgeStr string
	if hb > 0 {
		age := time.Duration(now - hb)
		hbAgeStr = fmt.Sprintf("%v ago", age.Round(time.Millisecond))
	} else {
		hbAgeStr = "never"
	}

	statusStr := "UNKNOWN"
	switch status {
	case shm.StatusBooting:
		statusStr = "BOOTING"
	case shm.StatusRunning:
		statusStr = "RUNNING"
	case shm.StatusHalting:
		statusStr = "HALTING"
	case shm.StatusClosed:
		statusStr = "CLOSED"
	}

	recModeStr := "ColdStart"
	switch recMode {
	case shm.RecoveryModeWarmSubCadence:
		recModeStr = "WarmSubCadence"
	case shm.RecoveryModeWarmResidentGap:
		recModeStr = "WarmResidentGap"
	}

	// ANSI clear screen and home cursor
	fmt.Print("\033[H\033[2J")
	fmt.Println("========================================================================================")
	fmt.Printf(" TICKHUB STATUS: %s (PID: %d) | SHM: /dev/shm/%s\n", statusStr, pid, name)
	fmt.Printf(" Generation: %d (%s) | BootID: 0x%016X | Heartbeat: %s\n", gen, recModeStr, bootID, hbAgeStr)
	fmt.Println("========================================================================================")
	fmt.Printf(" 1Hz PUBLISHING TELEMETRY:\n")
	fmt.Printf("  Last Anchor:     %d (%s)\n", lastAnchorNS, time.Unix(0, lastAnchorNS).UTC().Format(time.RFC3339))
	fmt.Printf("  Publish Latency: %.2f ms\n", float64(pubLatNS)/1e6)
	fmt.Printf("  Watermark Buffer:%.2f ms\n", float64(watermarkNS)/1e6)
	fmt.Printf("  Ticks Processed: %d | Dropped: %d\n", total, dropped)
	fmt.Println("----------------------------------------------------------------------------------------")
	fmt.Printf(" TOP-OF-BOOK SNAPSHOTS (Sample):\n")
	fmt.Printf("  %-8s %10s %10s %12s %10s %10s\n", "SYMBOL", "BID", "ASK", "MIDPRICE", "SPREAD", "LAST TRADE")

	raw := seg.Bytes()
	totalSymbols := int(hdr.TotalSymbols)
	if totalSymbols > 10 {
		totalSymbols = 10
	}

	for i := 0; i < totalSymbols; i++ {
		dirEntry := (*shm.SymbolDirectoryEntry)(unsafe.Pointer(&raw[shm.DirectoryOffset+uintptr(i*16)]))
		sym := strings.TrimRight(string(dirEntry.Name[:]), "\x00")
		if sym == "" {
			continue
		}
		snap := (*shm.SymbolSnapshot)(unsafe.Pointer(&raw[shm.SnapshotOffset+uintptr(i*128)]))

		// SeqLock read
		seq1 := atomic.LoadUint64(&snap.SeqLockSeq)
		if seq1%2 != 0 {
			continue
		}
		bid := snap.BidPx
		ask := snap.AskPx
		mid := snap.Midprice
		spd := snap.Spread
		lt := snap.LastTradePx
		seq2 := atomic.LoadUint64(&snap.SeqLockSeq)
		if seq1 != seq2 {
			continue
		}

		fmt.Printf("  %-8s %10.2f %10.2f %12.4f %10.4f %10.2f\n", sym, bid, ask, mid, spd, lt)
	}
	fmt.Println("========================================================================================")
}
