package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/benwulfe-fb/tickhub/pkg/feed"
	"github.com/benwulfe-fb/tickhub/pkg/project"
	"github.com/benwulfe-fb/tickhub/pkg/shm"
)

func runWorker(args []string) {
	fs := flag.NewFlagSet("worker", flag.ExitOnError)
	configPath := fs.String("config", "examples/config_datalake.yaml", "Path to config.yaml")
	datalakeDir := fs.String("datalake", "/mnt/wc/datalake", "Root datalake directory")
	shmNameOverride := fs.String("shm-name", "", "Optional SHM segment name override (e.g. tickhub_worker_0)")
	fs.Parse(args)

	cfgData, err := os.ReadFile(*configPath)
	if err != nil {
		log.Fatalf("[WORKER] Failed to read config %s: %v", *configPath, err)
	}

	var rawCfg ConfigFile
	if err := yaml.Unmarshal(cfgData, &rawCfg); err != nil {
		log.Fatalf("[WORKER] Failed to parse config YAML: %v", err)
	}

	shmName := rawCfg.SHM.Name
	if *shmNameOverride != "" {
		shmName = *shmNameOverride
	}

	phaseConfigs := make([]shm.PhaseConfig, len(rawCfg.Phases))
	for i, p := range rawCfg.Phases {
		phaseConfigs[i] = shm.PhaseConfig{
			ID:       p.ID,
			Name:     p.Name,
			OffsetMS: p.OffsetMS,
			Symbols:  p.Symbols,
		}
	}

	shmCfg := shm.Config{
		Name:            shmName,
		MaxFrames:       rawCfg.SHM.MaxFrames,
		CadenceInterval: time.Duration(rawCfg.SHM.CadenceInterval) * time.Nanosecond,
		UniqueSymbols:   rawCfg.UniqueSymbols,
		Features:        rawCfg.Features,
		Phases:          phaseConfigs,
		Permissions:     0666,
		UnlinkOnExit:    true,
		Mode:            shm.ModeHistoricalReplay,
		RawYAML:         cfgData,
	}

	prod, err := shm.CreateProducer(shmCfg)
	if err != nil {
		log.Fatalf("[WORKER] CreateProducer failed for %s: %v", shmName, err)
	}
	defer prod.Close()

	header := prod.Header()
	prod.SetStatus(shm.StatusRunning)
	atomic.StoreUint32(&header.ControlResp.Status, shm.ControlStatusIdle)
	atomic.StoreUint64(&header.ControlResp.ResponseID, 0)

	log.Printf("[WORKER] Daemon initialized on /dev/shm/%s (PID: %d). Listening for chunk requests...",
		shmName, os.Getpid())

	var lastHandledReqID uint64 = 0

	for {
		// 1. Check client liveness in-place (B2 Deadlock prevention)
		consumerPID := atomic.LoadInt64(&header.ConsumerPID)
		if consumerPID > 0 {
			if err := syscall.Kill(int(consumerPID), 0); err == syscall.ESRCH {
				if atomic.LoadUint32(&header.ControlResp.Status) == shm.ControlStatusBusy {
					log.Printf("[WORKER] Client PID %d exited; reset StatusBusy -> StatusIdle on /dev/shm/%s",
						consumerPID, shmName)
					atomic.StoreUint32(&header.ControlResp.Status, shm.ControlStatusIdle)
				}
				atomic.StoreInt64(&header.ConsumerPID, 0)
			}
		}

		// 2. Poll ControlRequest
		cmd := atomic.LoadUint32(&header.ControlReq.Command)
		reqID := atomic.LoadUint64(&header.ControlReq.RequestID)

		if cmd == shm.CmdShutdown {
			log.Printf("[WORKER] Received CmdShutdown. Unlinking /dev/shm/%s and exiting cleanly.", shmName)
			break
		}

		if cmd == shm.CmdReplayChunk && reqID > lastHandledReqID {
			atomic.StoreUint32(&header.ControlResp.Status, shm.ControlStatusBusy)
			lastHandledReqID = reqID

			dateVal := atomic.LoadUint32(&header.ControlReq.Date)
			dateStr := fmt.Sprintf("%04d-%02d-%02d", dateVal/10000, (dateVal%10000)/100, dateVal%100)

			var symRaw [8]byte
			copy(symRaw[:], header.ControlReq.Symbol[:])
			sym := strings.TrimRight(string(symRaw[:]), "\x00")

			startNS := atomic.LoadInt64(&header.ControlReq.StartAnchorNS)
			endNS := atomic.LoadInt64(&header.ControlReq.EndAnchorNS)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			t0 := time.Now()
			numFrames, coldStart, firstAnchor, lastAnchor, err := serviceChunk(ctx, prod, phaseConfigs, rawCfg, *datalakeDir, sym, dateStr, startNS, endNS)
			cancel()
			elapsed := time.Since(t0)

			if err != nil {
				log.Printf("[WORKER] Chunk req #%d failed: %v", reqID, err)
				for i := range header.ControlResp.ErrorMsg {
					header.ControlResp.ErrorMsg[i] = 0
				}
				errStr := err.Error()
				if len(errStr) > 23 {
					errStr = errStr[:23]
				}
				copy(header.ControlResp.ErrorMsg[:], errStr)
				atomic.StoreUint32(&header.ControlResp.Status, shm.ControlStatusError)
				atomic.StoreUint64(&header.ControlResp.ResponseID, reqID)
			} else {
				log.Printf("[WORKER] Servicing chunk req #%d %s %s [%d..%d] (%d frames in %v, cold_start=%d)",
					reqID, sym, dateStr, startNS, endNS, numFrames, elapsed, coldStart)
				atomic.StoreUint32(&header.ControlResp.NumFramesWritten, uint32(numFrames))
				atomic.StoreUint32(&header.ControlResp.ColdStartFrames, uint32(coldStart))
				atomic.StoreInt64(&header.ControlResp.FirstAnchorNS, firstAnchor)
				atomic.StoreInt64(&header.ControlResp.LastAnchorNS, lastAnchor)
				atomic.StoreUint32(&header.ControlResp.Status, shm.ControlStatusReady)
				atomic.StoreUint64(&header.ControlResp.ResponseID, reqID)
			}
		}

		time.Sleep(50 * time.Microsecond)
	}
}

func serviceChunk(
	ctx context.Context,
	prod *shm.Producer,
	phases []shm.PhaseConfig,
	rawCfg ConfigFile,
	datalakeDir string,
	sym string,
	dateStr string,
	startNS int64,
	endNS int64,
) (int, int, int64, int64, error) {
	letter := strings.ToUpper(string(sym[0]))
	dayDir := filepath.Join(datalakeDir, dateStr, letter)
	tradePath := filepath.Join(dayDir, fmt.Sprintf("%s.trades.parquet", sym))
	quotePath := filepath.Join(dayDir, fmt.Sprintf("%s.quotes.parquet", sym))

	var readers []feed.TickReader
	if _, err := os.Stat(tradePath); err == nil {
		tr, err := feed.OpenTradeReader(tradePath, sym, 4096)
		if err != nil {
			return 0, 0, 0, 0, fmt.Errorf("open trades: %w", err)
		}
		readers = append(readers, tr)
	}
	if _, err := os.Stat(quotePath); err == nil {
		qr, err := feed.OpenQuoteReader(quotePath, sym, 4096)
		if err != nil {
			return 0, 0, 0, 0, fmt.Errorf("open quotes: %w", err)
		}
		readers = append(readers, qr)
	}

	if len(readers) == 0 {
		return 0, 0, 0, 0, fmt.Errorf("no Parquet files found for symbol %s on %s in %s", sym, dateStr, dayDir)
	}

	merger := feed.NewMerger(readers)
	defer merger.Close()

	cadence := time.Duration(rawCfg.SHM.CadenceInterval) * time.Nanosecond
	cadenceNS := int64(cadence)
	if cadenceNS <= 0 {
		cadenceNS = int64(time.Second)
	}

	prod.ResetAnchors(startNS + cadenceNS)

	projector := project.NewProjector(prod, phases, rawCfg.UniqueSymbols, cadence)
	projector.SetStartAnchor(startNS)

	// 1. Warm-up pre-window sweep strictly before startNS
	var lastBid, lastAsk, lastPrice float64
	hasPreQuote := false
	hasPreTrade := false

	hasWindowTicks := false

	for merger.Next() {
		if err := ctx.Err(); err != nil {
			return 0, 0, 0, 0, fmt.Errorf("chunk replay timeout (5s): %w", err)
		}

		tick := merger.Tick()
		ts := tick.SIPTimestampNS

		if ts < startNS {
			if tick.Type == feed.TickQuote {
				if tick.BidPx > 0 {
					lastBid = tick.BidPx
				}
				if tick.AskPx > 0 {
					lastAsk = tick.AskPx
				}
				hasPreQuote = true
			} else {
				if tick.Price > 0 {
					lastPrice = tick.Price
				}
				hasPreTrade = true
			}
			continue
		}

		// First tick at or after startNS
		hasWindowTicks = true

		if hasPreQuote || hasPreTrade {
			projector.SeedState(sym, lastBid, lastAsk, lastPrice)
		}

		if ts < endNS {
			if err := projector.IngestTick(tick); err != nil {
				return 0, 0, 0, 0, fmt.Errorf("ingest tick @ %d: %w", ts, err)
			}
		}

		// Ingest remaining ticks within window
		for merger.Next() {
			if err := ctx.Err(); err != nil {
				return 0, 0, 0, 0, fmt.Errorf("chunk replay timeout (5s): %w", err)
			}
			t := merger.Tick()
			if t.SIPTimestampNS >= endNS {
				break
			}
			if err := projector.IngestTick(t); err != nil {
				return 0, 0, 0, 0, fmt.Errorf("ingest tick @ %d: %w", t.SIPTimestampNS, err)
			}
		}

		// Flush up to endNS
		if err := projector.Flush(endNS); err != nil {
			return 0, 0, 0, 0, fmt.Errorf("flush @ %d: %w", endNS, err)
		}
		break
	}

	if !hasWindowTicks {
		// No ticks at or after startNS; forward fill empty bars
		if hasPreQuote || hasPreTrade {
			projector.SeedState(sym, lastBid, lastAsk, lastPrice)
		}
		if err := projector.Flush(endNS); err != nil {
			return 0, 0, 0, 0, fmt.Errorf("flush empty @ %d: %w", endNS, err)
		}
	}

	coldStart := 0
	if !hasPreQuote {
		coldStart = 1
	}

	numFrames := projector.CommittedFrames()
	firstAnchor, lastAnchor := projector.AnchorRange()
	return numFrames, coldStart, firstAnchor, lastAnchor, nil
}
