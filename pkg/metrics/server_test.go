package metrics

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benwulfe-fb/tickhub/pkg/shm"
)

func TestMetricsServerEndpoints(t *testing.T) {
	hdr := &shm.GlobalHeader{
		Status:                 shm.StatusBooting,
		Mode:                   shm.ModeLiveStreaming,
		Generation:             1,
		RecoveryMode:           shm.RecoveryModeWarmSubCadence,
		BootID:                 0x12345678,
		AnchorPublishLatencyNS: 1_250_000,
		WatermarkBufferNS:      50_000_000,
		LastWrittenAnchorNS:    1700000000_000_000_000,
		TotalTickCount:         1500,
		DroppedTickCount:       2,
		HeartbeatNS:            time.Now().UnixNano(),
	}

	srv := NewServer("127.0.0.1:0", hdr)
	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Stop(ctx)
	}()

	addr := srv.Addr()
	client := &http.Client{Timeout: 2 * time.Second}

	// 1. Test /healthz
	resp, err := client.Get("http://" + addr + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from /healthz, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// 2. Test /readyz while booting (should be 503)
	resp, err = client.Get("http://" + addr + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz failed: %v", err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 from /readyz during booting, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// 3. Set StatusRunning and fresh heartbeat -> should be 200
	atomic.StoreUint32(&hdr.Status, shm.StatusRunning)
	atomic.StoreInt64(&hdr.HeartbeatNS, time.Now().UnixNano())

	resp, err = client.Get("http://" + addr + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from /readyz when running, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// 4. Stale heartbeat (> 3s) -> should be 503
	atomic.StoreInt64(&hdr.HeartbeatNS, time.Now().UnixNano()-4_000_000_000)
	resp, err = client.Get("http://" + addr + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz failed: %v", err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 from /readyz when heartbeat stale, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// 5. Test /metrics text output
	srv.SetRecoveryStats(500_000_000, 1)
	srv.SetCommittedFramesFunc(func() int { return 42 })

	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)

	metricsBody := w.Body.String()
	expectedSubstrings := []string{
		"tickhub_uptime_seconds",
		"tickhub_daemon_status 2",
		"tickhub_daemon_generation 1",
		"tickhub_daemon_recovery_mode 1",
		"tickhub_boot_id 305419896",
		"tickhub_publish_latency_nanoseconds 1250000",
		"tickhub_watermark_buffer_nanoseconds 50000000",
		"tickhub_ticks_total 1500",
		"tickhub_ticks_dropped_total 2",
		"tickhub_committed_anchors_total 42",
		"tickhub_recovery_downtime_nanoseconds 500000000",
		"tickhub_recovery_missed_anchors_total 1",
	}

	for _, sub := range expectedSubstrings {
		if !strings.Contains(metricsBody, sub) {
			t.Errorf("metrics missing expected line %q:\n%s", sub, metricsBody)
		}
	}
}

func TestMetricsServerNilHeader(t *testing.T) {
	srv := NewServer(":0", nil)
	req := httptest.NewRequest("GET", "/readyz", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for nil header readyz, got %d", w.Code)
	}

	reqMetrics := httptest.NewRequest("GET", "/metrics", nil)
	wMetrics := httptest.NewRecorder()
	srv.Handler().ServeHTTP(wMetrics, reqMetrics)

	body, _ := io.ReadAll(wMetrics.Body)
	if !strings.Contains(string(body), "tickhub_daemon_status 0") {
		t.Fatalf("expected status 0 for nil header, got %s", string(body))
	}
}

type mockFeedController struct {
	mu      sync.Mutex
	enabled bool
	ticks   uint64
}

func (m *mockFeedController) FeedStatus() (bool, uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.enabled, m.ticks
}

func (m *mockFeedController) EnableFeed(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.enabled = true
	return nil
}

func (m *mockFeedController) DisableFeed(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.enabled = false
	return nil
}

func TestControlFeedEndpoint(t *testing.T) {
	srv := NewServer(":0", nil)

	// 1. Without feed controller
	req := httptest.NewRequest("GET", "/control/feed", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501 without feed controller, got %d", w.Code)
	}

	// 2. Attach mock controller
	mock := &mockFeedController{enabled: false, ticks: 100}
	srv.SetFeedController(mock)

	// GET status
	req = httptest.NewRequest("GET", "/control/feed", nil)
	w = httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for GET /control/feed, got %d", w.Code)
	}
	body, _ := io.ReadAll(w.Body)
	if !strings.Contains(string(body), `"enabled":false`) || !strings.Contains(string(body), `"ticks":100`) {
		t.Fatalf("unexpected body: %s", string(body))
	}

	// POST enable
	req = httptest.NewRequest("POST", "/control/feed?action=enable", nil)
	w = httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for POST enable, got %d", w.Code)
	}
	if !mock.enabled {
		t.Fatalf("expected controller enabled=true")
	}

	// POST disable
	req = httptest.NewRequest("POST", "/control/feed?action=disable", nil)
	w = httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for POST disable, got %d", w.Code)
	}
	if mock.enabled {
		t.Fatalf("expected controller enabled=false")
	}

	// Invalid action
	req = httptest.NewRequest("POST", "/control/feed?action=invalid", nil)
	w = httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid action, got %d", w.Code)
	}
}
