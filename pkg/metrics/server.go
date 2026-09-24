package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benwulfe-fb/tickhub/pkg/shm"
)

// FeedController defines dynamic control over the market data ingestion feed.
type FeedController interface {
	FeedStatus() (enabled bool, ticks uint64)
	EnableFeed(ctx context.Context) error
	DisableFeed(ctx context.Context) error
}

// Server provides HTTP observability endpoints (/metrics, /healthz, /readyz, /control/feed).
type Server struct {
	addr              string
	hdr               *shm.GlobalHeader
	startTime         time.Time
	srv               *http.Server
	ln                net.Listener
	downtimeNS        int64
	missedAnchors     uint64
	committedFramesFn func() int
	feedCtrl          FeedController
	feedMu            sync.RWMutex
}

// SetRecoveryStats sets downtime and missed anchors telemetry for Prometheus export.
func (s *Server) SetRecoveryStats(downtimeNS int64, missedAnchors uint64) {
	s.downtimeNS = downtimeNS
	s.missedAnchors = missedAnchors
}

// SetCommittedFramesFunc attaches a provider function for total committed 1Hz frames.
func (s *Server) SetCommittedFramesFunc(fn func() int) {
	s.committedFramesFn = fn
}

// SetFeedController attaches a dynamic feed controller for runtime start/stop.
func (s *Server) SetFeedController(fc FeedController) {
	s.feedMu.Lock()
	defer s.feedMu.Unlock()
	s.feedCtrl = fc
}

// NewServer initializes an HTTP metrics and health monitoring server.
func NewServer(addr string, hdr *shm.GlobalHeader) *Server {
	if addr == "" {
		addr = ":9090"
	}
	s := &Server{
		addr:      addr,
		hdr:       hdr,
		startTime: time.Now(),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/control/feed", s.handleControlFeed)

	s.srv = &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	return s
}

// Handler returns the underlying http.Handler for direct testing.
func (s *Server) Handler() http.Handler {
	return s.srv.Handler
}

// Start begins listening and serving in the background.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("metrics server listen %s: %w", s.addr, err)
	}
	s.ln = ln

	go func() {
		_ = s.srv.Serve(ln)
	}()
	return nil
}

// Stop gracefully shuts down the HTTP server.
func (s *Server) Stop(ctx context.Context) error {
	if s.srv != nil {
		return s.srv.Shutdown(ctx)
	}
	return nil
}

// Addr returns the active listener address (useful when bound to :0).
func (s *Server) Addr() string {
	if s.ln != nil {
		return s.ln.Addr().String()
	}
	return s.addr
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK\n"))
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if s.hdr == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("NOT READY: No SHM header attached\n"))
		return
	}

	status := atomic.LoadUint32(&s.hdr.Status)
	if status != shm.StatusRunning {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = fmt.Fprintf(w, "NOT READY: Daemon status=%d (expected %d)\n", status, shm.StatusRunning)
		return
	}

	hb := atomic.LoadInt64(&s.hdr.HeartbeatNS)
	now := time.Now().UnixNano()
	if hb > 0 && (now-hb) > 3_000_000_000 {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = fmt.Fprintf(w, "NOT READY: Heartbeat stale (age: %v)\n", time.Duration(now-hb))
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("READY\n"))
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	uptimeSec := time.Since(s.startTime).Seconds()
	var (
		status       uint32
		mode         uint32
		gen          uint32
		recMode      uint32
		pubLatNS     int64
		watermarkNS  int64
		lastAnchorNS int64
		droppedTicks uint64
		totalTicks   uint64
		bootID       uint64
	)

	if s.hdr != nil {
		status = atomic.LoadUint32(&s.hdr.Status)
		mode = atomic.LoadUint32(&s.hdr.Mode)
		gen = atomic.LoadUint32(&s.hdr.Generation)
		recMode = atomic.LoadUint32(&s.hdr.RecoveryMode)
		pubLatNS = atomic.LoadInt64(&s.hdr.AnchorPublishLatencyNS)
		watermarkNS = atomic.LoadInt64(&s.hdr.WatermarkBufferNS)
		lastAnchorNS = atomic.LoadInt64(&s.hdr.LastWrittenAnchorNS)
		droppedTicks = atomic.LoadUint64(&s.hdr.DroppedTickCount)
		totalTicks = atomic.LoadUint64(&s.hdr.TotalTickCount)
		bootID = atomic.LoadUint64(&s.hdr.BootID)
	}

	_, _ = fmt.Fprintf(w, "# HELP tickhub_uptime_seconds Daemon uptime in seconds\n")
	_, _ = fmt.Fprintf(w, "# TYPE tickhub_uptime_seconds gauge\n")
	_, _ = fmt.Fprintf(w, "tickhub_uptime_seconds %.2f\n\n", uptimeSec)

	_, _ = fmt.Fprintf(w, "# HELP tickhub_daemon_status Current daemon operational status\n")
	_, _ = fmt.Fprintf(w, "# TYPE tickhub_daemon_status gauge\n")
	_, _ = fmt.Fprintf(w, "tickhub_daemon_status %d\n\n", status)

	_, _ = fmt.Fprintf(w, "# HELP tickhub_daemon_mode Operational mode (0=Live, 1=Replay)\n")
	_, _ = fmt.Fprintf(w, "# TYPE tickhub_daemon_mode gauge\n")
	_, _ = fmt.Fprintf(w, "tickhub_daemon_mode %d\n\n", mode)

	_, _ = fmt.Fprintf(w, "# HELP tickhub_daemon_generation Monotonic SHM segment generation\n")
	_, _ = fmt.Fprintf(w, "# TYPE tickhub_daemon_generation counter\n")
	_, _ = fmt.Fprintf(w, "tickhub_daemon_generation %d\n\n", gen)

	_, _ = fmt.Fprintf(w, "# HELP tickhub_daemon_recovery_mode Recovery mode (0=Cold, 1=WarmSubCadence, 2=WarmResidentGap)\n")
	_, _ = fmt.Fprintf(w, "# TYPE tickhub_daemon_recovery_mode gauge\n")
	_, _ = fmt.Fprintf(w, "tickhub_daemon_recovery_mode %d\n\n", recMode)

	_, _ = fmt.Fprintf(w, "# HELP tickhub_boot_id Daemon random boot UUID\n")
	_, _ = fmt.Fprintf(w, "# TYPE tickhub_boot_id gauge\n")
	_, _ = fmt.Fprintf(w, "tickhub_boot_id %d\n\n", bootID)

	_, _ = fmt.Fprintf(w, "# HELP tickhub_publish_latency_nanoseconds Publishing latency after bar anchor\n")
	_, _ = fmt.Fprintf(w, "# TYPE tickhub_publish_latency_nanoseconds gauge\n")
	_, _ = fmt.Fprintf(w, "tickhub_publish_latency_nanoseconds %d\n\n", pubLatNS)

	_, _ = fmt.Fprintf(w, "# HELP tickhub_watermark_buffer_nanoseconds Watermark buffer latency\n")
	_, _ = fmt.Fprintf(w, "# TYPE tickhub_watermark_buffer_nanoseconds gauge\n")
	_, _ = fmt.Fprintf(w, "tickhub_watermark_buffer_nanoseconds %d\n\n", watermarkNS)

	_, _ = fmt.Fprintf(w, "# HELP tickhub_last_written_anchor_nanoseconds Timestamp of last committed anchor\n")
	_, _ = fmt.Fprintf(w, "# TYPE tickhub_last_written_anchor_nanoseconds gauge\n")
	_, _ = fmt.Fprintf(w, "tickhub_last_written_anchor_nanoseconds %d\n\n", lastAnchorNS)

	_, _ = fmt.Fprintf(w, "# HELP tickhub_ticks_total Total ticks processed\n")
	_, _ = fmt.Fprintf(w, "# TYPE tickhub_ticks_total counter\n")
	_, _ = fmt.Fprintf(w, "tickhub_ticks_total %d\n\n", totalTicks)

	_, _ = fmt.Fprintf(w, "# HELP tickhub_ticks_dropped_total Total ticks dropped\n")
	_, _ = fmt.Fprintf(w, "# TYPE tickhub_ticks_dropped_total counter\n")
	_, _ = fmt.Fprintf(w, "tickhub_ticks_dropped_total %d\n\n", droppedTicks)

	var committedFrames int
	if s.committedFramesFn != nil {
		committedFrames = s.committedFramesFn()
	}
	_, _ = fmt.Fprintf(w, "# HELP tickhub_committed_anchors_total Total 1Hz frames finalized\n")
	_, _ = fmt.Fprintf(w, "# TYPE tickhub_committed_anchors_total counter\n")
	_, _ = fmt.Fprintf(w, "tickhub_committed_anchors_total %d\n\n", committedFrames)

	_, _ = fmt.Fprintf(w, "# HELP tickhub_recovery_downtime_nanoseconds Duration daemon was down prior to restart\n")
	_, _ = fmt.Fprintf(w, "# TYPE tickhub_recovery_downtime_nanoseconds gauge\n")
	_, _ = fmt.Fprintf(w, "tickhub_recovery_downtime_nanoseconds %d\n\n", s.downtimeNS)

	_, _ = fmt.Fprintf(w, "# HELP tickhub_recovery_missed_anchors_total Total 1Hz anchors missed during downtime\n")
	_, _ = fmt.Fprintf(w, "# TYPE tickhub_recovery_missed_anchors_total counter\n")
	_, _ = fmt.Fprintf(w, "tickhub_recovery_missed_anchors_total %d\n", s.missedAnchors)
}

func (s *Server) handleControlFeed(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	s.feedMu.RLock()
	fc := s.feedCtrl
	s.feedMu.RUnlock()

	if fc == nil {
		w.WriteHeader(http.StatusNotImplemented)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": "feed controller not registered"})
		return
	}

	if r.Method == http.MethodGet {
		enabled, ticks := fc.FeedStatus()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":  "ok",
			"enabled": enabled,
			"ticks":   ticks,
		})
		return
	}

	if r.Method == http.MethodPost {
		action := r.URL.Query().Get("action")
		var err error
		switch action {
		case "enable":
			err = fc.EnableFeed(r.Context())
		case "disable":
			err = fc.DisableFeed(r.Context())
		default:
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": "invalid action; expected ?action=enable or ?action=disable",
			})
			return
		}

		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": err.Error(),
			})
			return
		}

		enabled, ticks := fc.FeedStatus()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":  "ok",
			"action":  action,
			"enabled": enabled,
			"ticks":   ticks,
		})
		return
	}

	w.WriteHeader(http.StatusMethodNotAllowed)
}
