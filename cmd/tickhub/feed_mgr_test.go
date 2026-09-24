package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/benwulfe-fb/tickhub/pkg/feed"
	"github.com/benwulfe-fb/tickhub/pkg/shm"
)

func TestFeedManagerLifecycle(t *testing.T) {
	fm := NewFeedManager(context.Background(), "wss://socket.massive.com/stocks", "", []string{"AAPL", "MSFT"})

	// Initially disabled
	enabled, ticks := fm.FeedStatus()
	if enabled {
		t.Fatalf("expected feed initially disabled")
	}
	if ticks != 0 {
		t.Fatalf("expected 0 ticks initially")
	}

	// Enable without API key fails
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err := fm.EnableFeed(ctx)
	if err == nil {
		t.Fatalf("expected error enabling feed without API key")
	}

	// Disable when already disabled is idempotent
	if err := fm.DisableFeed(ctx); err != nil {
		t.Fatalf("unexpected error disabling feed: %v", err)
	}

	// Ticks channel is non-nil and readable
	select {
	case <-fm.Ticks():
		t.Fatalf("unexpected tick on disabled feed")
	case <-time.After(10 * time.Millisecond):
		// Expected: no ticks when disabled
	}
}

func TestFeedManagerReEnableCycle(t *testing.T) {
	upgrader := websocket.Upgrader{}

	// Setup mock WebSocket server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()

		// Read Auth
		_, authMsg, err := c.ReadMessage()
		if err != nil {
			return
		}
		var authData map[string]string
		_ = json.Unmarshal(authMsg, &authData)

		// Reply auth success
		_ = c.WriteJSON([]feed.RawMassiveEvent{
			{Ev: "status", Status: "auth_success", Msg: "authenticated"},
		})

		// Read Subscribe
		_, _, _ = c.ReadMessage()

		// Emit one quote
		_ = c.WriteJSON([]feed.RawMassiveEvent{
			{
				Ev: "Q", Sym: "AAPL", T: 1770000000000,
				Bp: 150.25, Ap: 150.30, Bs: 10, As: 12,
			},
		})

		// Hold connection open until client closes
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				break
			}
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	fm := NewFeedManager(context.Background(), wsURL, "TEST_KEY", []string{"AAPL"})

	// Cycle 1: Enable -> receive tick -> Disable
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := fm.EnableFeed(ctx); err != nil {
		t.Fatalf("cycle 1 EnableFeed failed: %v", err)
	}
	enabled, _ := fm.FeedStatus()
	if !enabled {
		t.Fatalf("cycle 1 expected enabled=true")
	}

	select {
	case tick := <-fm.Ticks():
		if tick.Symbol != "AAPL" || tick.BidPx != 150.25 {
			t.Fatalf("unexpected tick: %+v", tick)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("cycle 1 timed out waiting for tick")
	}

	if err := fm.DisableFeed(ctx); err != nil {
		t.Fatalf("cycle 1 DisableFeed failed: %v", err)
	}
	enabled, _ = fm.FeedStatus()
	if enabled {
		t.Fatalf("cycle 1 expected enabled=false after disable")
	}

	// Cycle 2: Re-enable -> receive tick -> Disable again
	if err := fm.EnableFeed(ctx); err != nil {
		t.Fatalf("cycle 2 EnableFeed failed: %v", err)
	}
	enabled, _ = fm.FeedStatus()
	if !enabled {
		t.Fatalf("cycle 2 expected enabled=true")
	}

	select {
	case tick := <-fm.Ticks():
		if tick.Symbol != "AAPL" {
			t.Fatalf("unexpected tick in cycle 2: %+v", tick)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("cycle 2 timed out waiting for tick")
	}

	if err := fm.DisableFeed(ctx); err != nil {
		t.Fatalf("cycle 2 DisableFeed failed: %v", err)
	}
	enabled, _ = fm.FeedStatus()
	if enabled {
		t.Fatalf("cycle 2 expected enabled=false after disable")
	}
}

func TestFeedManagerCallerContextCancellation(t *testing.T) {
	upgrader := websocket.Upgrader{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()

		_, _, _ = c.ReadMessage()
		_ = c.WriteJSON([]feed.RawMassiveEvent{
			{Ev: "status", Status: "auth_success", Msg: "authenticated"},
		})
		_, _, _ = c.ReadMessage()

		// Stream two ticks separated by 50ms
		time.Sleep(20 * time.Millisecond)
		_ = c.WriteJSON([]feed.RawMassiveEvent{
			{Ev: "Q", Sym: "MSFT", T: 1770000000000, Bp: 400.1, Ap: 400.2},
		})
		time.Sleep(50 * time.Millisecond)
		_ = c.WriteJSON([]feed.RawMassiveEvent{
			{Ev: "Q", Sym: "MSFT", T: 1770000001000, Bp: 400.3, Ap: 400.4},
		})

		for {
			if _, _, err := c.ReadMessage(); err != nil {
				break
			}
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	rootCtx, rootCancel := context.WithCancel(context.Background())
	defer rootCancel()

	fm := NewFeedManager(rootCtx, wsURL, "TEST_KEY", []string{"MSFT"})

	// Simulate HTTP handler: pass request context and cancel it immediately after EnableFeed returns
	reqCtx, reqCancel := context.WithCancel(context.Background())
	if err := fm.EnableFeed(reqCtx); err != nil {
		t.Fatalf("EnableFeed failed: %v", err)
	}
	reqCancel() // Cancel caller context immediately (mimicking HTTP request completion)

	// Verify that feed continues streaming because it is bound to rootCtx, not reqCtx
	gotTicks := 0
	deadline := time.After(2 * time.Second)
	for gotTicks < 2 {
		select {
		case tick := <-fm.Ticks():
			if tick.Symbol == "MSFT" {
				gotTicks++
			}
		case <-deadline:
			t.Fatalf("expected 2 ticks despite reqCtx cancellation, got %d", gotTicks)
		}
	}

	_ = fm.DisableFeed(context.Background())
}

func TestStandbyHeartbeatContinuity(t *testing.T) {
	shmCfg := shm.Config{
		Name:            "test_standby_heartbeat",
		MaxFrames:       64,
		CadenceInterval: 1 * time.Second,
		UniqueSymbols:   []string{"AAPL"},
		Features:        []string{"log_ret_1s"},
		Phases: []shm.PhaseConfig{
			{ID: 0, Name: "phase_0ms", OffsetMS: 0, Symbols: []string{"AAPL"}},
		},
		Permissions:  0666,
		UnlinkOnExit: true,
		Mode:         shm.ModeLiveStreaming,
	}

	prod, err := shm.CreateProducer(shmCfg)
	if err != nil {
		t.Fatalf("CreateProducer failed: %v", err)
	}
	defer prod.Close()

	// Initial heartbeat
	t0 := time.Now().UnixNano()
	prod.PublishTelemetry(0, 0, 0, 0)
	hb0 := atomic.LoadInt64(&prod.Header().HeartbeatNS)
	if hb0 < t0 {
		t.Fatalf("expected HeartbeatNS >= %d, got %d", t0, hb0)
	}

	// Sleep 50ms and publish heartbeat again while in standby (0 latency, 0 ticks)
	time.Sleep(50 * time.Millisecond)
	prod.PublishTelemetry(0, 0, 0, 0)
	hb1 := atomic.LoadInt64(&prod.Header().HeartbeatNS)
	if hb1 <= hb0 {
		t.Fatalf("expected HeartbeatNS to advance: hb0=%d, hb1=%d", hb0, hb1)
	}
}
