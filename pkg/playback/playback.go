package playback

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benwulfe-fb/tickhub/pkg/recorder"
	"github.com/benwulfe-fb/tickhub/pkg/shm"
	"github.com/parquet-go/parquet-go"
	"gopkg.in/yaml.v3"
)

type phaseRef struct {
	pIdx int
	sIdx int
}

// Engine manages historical 1Hz telemetry playback into POSIX SHM.
type Engine struct {
	cfg         Config
	mu          sync.Mutex
	prod        *shm.Producer
	stats       Stats
	files       []string
	symToIdx    map[string]int
	symPhaseMap map[string]phaseRef
	closed      atomic.Bool
}

// NewEngine creates and configures a new playback engine.
func NewEngine(cfg Config) (*Engine, error) {
	if cfg.SHMName == "" {
		cfg.SHMName = "tickhub_playback"
	}
	if cfg.FlowControlTimeout <= 0 {
		cfg.FlowControlTimeout = 10 * time.Second
	}

	cleanName := strings.TrimPrefix(cfg.SHMName, "/dev/shm/")
	cleanName = strings.TrimPrefix(cleanName, "tickhub_")
	cfg.SHMName = cleanName

	files, err := discoverParquetFiles(cfg.SourcePath)
	if err != nil {
		return nil, fmt.Errorf("discover parquet files: %w", err)
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no parquet files found at: %s", cfg.SourcePath)
	}

	return &Engine{
		cfg:         cfg,
		files:       files,
		symToIdx:    make(map[string]int),
		symPhaseMap: make(map[string]phaseRef),
	}, nil
}

func discoverParquetFiles(path string) ([]string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	if !info.IsDir() {
		return []string{path}, nil
	}

	var files []string
	err = filepath.Walk(path, func(p string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !fi.IsDir() && strings.HasSuffix(strings.ToLower(fi.Name()), ".parquet") {
			files = append(files, p)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	return files, nil
}

type yamlConfigFile struct {
	SHM struct {
		Name            string `yaml:"name"`
		MaxFrames       uint32 `yaml:"max_frames"`
		CadenceInterval int64  `yaml:"cadence_interval"`
	} `yaml:"shm"`
	Features      []string `yaml:"features"`
	UniqueSymbols []string `yaml:"unique_symbols"`
	Phases        []struct {
		ID       uint32   `yaml:"id"`
		Name     string   `yaml:"name"`
		OffsetMS uint32   `yaml:"offset_ms"`
		Symbols  []string `yaml:"symbols"`
	} `yaml:"phases"`
}

func (e *Engine) initProducerFromFile(firstFile string) (*shm.Producer, error) {
	f, err := os.Open(firstFile)
	if err != nil {
		return nil, fmt.Errorf("open first file %s: %w", firstFile, err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat first file %s: %w", firstFile, err)
	}

	pf, err := parquet.OpenFile(f, stat.Size())
	if err != nil {
		return nil, fmt.Errorf("parquet open %s: %w", firstFile, err)
	}

	configYAML, hasYAML := pf.Lookup("tickhub_config_yaml")
	var shmCfg shm.Config

	if hasYAML && len(configYAML) > 0 {
		var rawCfg yamlConfigFile
		if err := yaml.Unmarshal([]byte(configYAML), &rawCfg); err == nil {
			phaseConfigs := make([]shm.PhaseConfig, len(rawCfg.Phases))
			for i, p := range rawCfg.Phases {
				phaseConfigs[i] = shm.PhaseConfig{
					ID:       p.ID,
					Name:     p.Name,
					OffsetMS: p.OffsetMS,
					Symbols:  p.Symbols,
				}
			}

			cadence := time.Duration(rawCfg.SHM.CadenceInterval) * time.Nanosecond
			if cadence <= 0 {
				cadence = time.Second
			}
			maxFrames := rawCfg.SHM.MaxFrames
			if maxFrames == 0 {
				maxFrames = 1024
			}

			shmCfg = shm.Config{
				Name:            e.cfg.SHMName,
				MaxFrames:       maxFrames,
				CadenceInterval: cadence,
				UniqueSymbols:   rawCfg.UniqueSymbols,
				Features:        rawCfg.Features,
				Phases:          phaseConfigs,
				Permissions:     0666,
				UnlinkOnExit:    e.cfg.UnlinkOnExit,
				Mode:            shm.ModeHistoricalReplay,
				RawYAML:         []byte(configYAML),
			}
		}
	}

	// Fallback to inspecting rows if config YAML is missing or unparseable
	if len(shmCfg.UniqueSymbols) == 0 {
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return nil, fmt.Errorf("seek file start: %w", err)
		}
		reader := parquet.NewGenericReader[recorder.MetricRow](f)
		sample := make([]recorder.MetricRow, 4096)
		n, _ := reader.Read(sample)
		_ = reader.Close()

		symSet := make(map[string]bool)
		var symList []string
		numFeat := 5
		for i := 0; i < n; i++ {
			s := sample[i].Symbol
			if !symSet[s] && s != "" {
				symSet[s] = true
				symList = append(symList, s)
			}
			if len(sample[i].Features) > numFeat {
				numFeat = len(sample[i].Features)
			}
		}

		featNames := make([]string, numFeat)
		for i := 0; i < numFeat; i++ {
			featNames[i] = fmt.Sprintf("feat_%d", i)
		}

		shmCfg = shm.Config{
			Name:            e.cfg.SHMName,
			MaxFrames:       1024,
			CadenceInterval: time.Second,
			UniqueSymbols:   symList,
			Features:        featNames,
			Phases: []shm.PhaseConfig{
				{
					ID:       0,
					Name:     "default",
					OffsetMS: 0,
					Symbols:  symList,
				},
			},
			Permissions:  0666,
			UnlinkOnExit: e.cfg.UnlinkOnExit,
			Mode:         shm.ModeHistoricalReplay,
		}
	}

	prod, err := shm.CreateProducer(shmCfg)
	if err != nil {
		return nil, fmt.Errorf("create SHM producer: %w", err)
	}

	e.symToIdx = make(map[string]int, len(shmCfg.UniqueSymbols))
	for i, s := range shmCfg.UniqueSymbols {
		e.symToIdx[s] = i
	}

	e.symPhaseMap = make(map[string]phaseRef)
	for pIdx, p := range shmCfg.Phases {
		for sIdx, s := range p.Symbols {
			e.symPhaseMap[s] = phaseRef{pIdx: pIdx, sIdx: sIdx}
		}
	}

	return prod, nil
}

// Run executes the replay stream across all configured Parquet files.
func (e *Engine) Run(ctx context.Context) error {
	prod, err := e.initProducerFromFile(e.files[0])
	if err != nil {
		return err
	}
	e.prod = prod
	defer prod.Close()

	prod.SetStatus(shm.StatusRunning)
	startWall := time.Now()
	var firstAnchorNS int64

	for {
		for _, filePath := range e.files {
			if ctx.Err() != nil || e.closed.Load() {
				prod.SetStatus(shm.StatusClosed)
				return nil
			}

			if err := e.replayFile(ctx, filePath, &firstAnchorNS); err != nil {
				if ctx.Err() != nil || e.closed.Load() {
					prod.SetStatus(shm.StatusClosed)
					return nil
				}
				return fmt.Errorf("replay file %s: %w", filePath, err)
			}
		}

		if !e.cfg.Loop || ctx.Err() != nil || e.closed.Load() {
			break
		}

		// Reset anchors for monotonic loop restart
		if firstAnchorNS > 0 {
			prod.ResetAnchors(firstAnchorNS)
		}
	}

	e.mu.Lock()
	e.stats.ElapsedTime = time.Since(startWall)
	e.mu.Unlock()

	prod.SetStatus(shm.StatusClosed)
	return nil
}

type anchorGroup struct {
	anchorNS int64
	rows     []recorder.MetricRow
}

func (e *Engine) replayFile(ctx context.Context, path string, firstAnchorNS *int64) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	reader := parquet.NewGenericReader[recorder.MetricRow](f)
	defer reader.Close()

	batch := make([]recorder.MetricRow, 1024)
	var currentGroup *anchorGroup
	var prevAnchorNS int64

	commitGroup := func(grp *anchorGroup) error {
		if grp == nil || len(grp.rows) == 0 {
			return nil
		}
		anchorNS := grp.anchorNS
		if *firstAnchorNS == 0 {
			*firstAnchorNS = anchorNS
		}

		// Flow control: Wait for consumer advance before overwriting ring slots
		waitStart := time.Now()
		if err := e.prod.WaitConsumerAdvance(anchorNS, e.cfg.FlowControlTimeout); err != nil {
			return fmt.Errorf("flow control wait @ %d: %w", anchorNS, err)
		}
		waitNS := time.Since(waitStart).Nanoseconds()

		// Write snapshots and collect phase features
		for _, row := range grp.rows {
			symIdx, ok := e.symToIdx[row.Symbol]
			if !ok {
				continue
			}

			snap := shm.SymbolSnapshot{
				SeqLockSeq:      row.SeqLockSeq,
				SIPTimestampNS:  row.TimestampNS,
				RecvTimestampNS: row.RecvTSNS,
				BidPx:           row.BidPX,
				AskPx:           row.AskPX,
				BidSz:           row.BidSZ,
				AskSz:           row.AskSZ,
				Midprice:        row.MidPX,
				Spread:          row.SpreadPX,
				LastTradePx:     row.LastTradePX,
				LastTradeSz:     row.LastTradeSZ,
				BidExch:         row.BidExch,
				AskExch:         row.AskExch,
				TradeExch:       row.TradeExch,
				Conditions:      row.Conditions,
			}
			e.prod.WriteSnapshot(symIdx, &snap)

			if len(row.Features) > 0 {
				if pref, ok := e.symPhaseMap[row.Symbol]; ok {
					if err := e.prod.CommitSymbolMetrics(pref.pIdx, pref.sIdx, anchorNS, row.Features); err != nil {
						return fmt.Errorf("commit symbol metrics %s (p%d, s%d) @ %d: %w",
							row.Symbol, pref.pIdx, pref.sIdx, anchorNS, err)
					}
				}
			}
		}

		e.prod.CommitFrameFinalize(anchorNS)

		e.mu.Lock()
		e.stats.AnchorsReplayed++
		e.stats.TotalRows += int64(len(grp.rows))
		e.stats.FlowControlWaitNS += waitNS
		e.mu.Unlock()

		// Speed pacing
		if prevAnchorNS > 0 && e.cfg.Speed > 0 {
			deltaNS := anchorNS - prevAnchorNS
			if deltaNS > 0 {
				sleepDur := time.Duration(float64(deltaNS) / e.cfg.Speed)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(sleepDur):
				}
			}
		}
		prevAnchorNS = anchorNS
		return nil
	}

	for {
		if ctx.Err() != nil || e.closed.Load() {
			return nil
		}

		n, err := reader.Read(batch)
		if n == 0 {
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			break
		}

		for i := 0; i < n; i++ {
			row := batch[i]
			if currentGroup == nil {
				currentGroup = &anchorGroup{
					anchorNS: row.TimestampNS,
					rows:     []recorder.MetricRow{row},
				}
			} else if row.TimestampNS == currentGroup.anchorNS {
				currentGroup.rows = append(currentGroup.rows, row)
			} else {
				if err := commitGroup(currentGroup); err != nil {
					return err
				}
				currentGroup = &anchorGroup{
					anchorNS: row.TimestampNS,
					rows:     []recorder.MetricRow{row},
				}
			}
		}
	}

	if currentGroup != nil && len(currentGroup.rows) > 0 {
		if err := commitGroup(currentGroup); err != nil {
			return err
		}
	}

	return nil
}

// Stats returns playback telemetry thread-safely.
func (e *Engine) Stats() Stats {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.stats
}

// Close terminates playback and frees resources.
func (e *Engine) Close() error {
	e.closed.Store(true)
	return nil
}
