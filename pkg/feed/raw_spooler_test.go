package feed

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/klauspost/compress/zstd"
)

func TestRawSpoolerRecordAndFlush(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tickhub_spool_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	spooler, err := NewRawSpooler(RawSpoolerConfig{
		Dir:        tempDir,
		BufferSize: 1024,
		FlushTick:  50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewRawSpooler failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	spooler.Start(ctx)

	// Send 5 test raw frames
	testFrames := [][]byte{
		[]byte(`[{"ev":"status","status":"auth_success","message":"authenticated"}]`),
		[]byte(`[{"ev":"Q","sym":"AAPL","bp":220.5,"ap":220.6,"bs":10,"as":12,"t":1728678000100}]`),
		[]byte(`[{"ev":"T","sym":"AAPL","p":220.55,"s":100,"t":1728678000105}]`),
		[]byte(`[{"ev":"Q","sym":"SPY","bp":575.1,"ap":575.2,"bs":50,"as":50,"t":1728678000110}]`),
		[]byte(`[{"ev":"T","sym":"SPY","p":575.15,"s":200,"t":1728678000115}]`),
	}

	nowNS := time.Now().UnixNano()
	for _, frame := range testFrames {
		ok := spooler.Record(nowNS, frame)
		if !ok {
			t.Fatalf("spooler.Record failed for frame %s", string(frame))
		}
	}

	// Graceful close flushes all frames
	if err := spooler.Close(); err != nil {
		t.Fatalf("spooler.Close failed: %v", err)
	}

	bytesW, packetsW, dropped := spooler.Stats()
	if packetsW != 5 {
		t.Errorf("expected 5 packets written, got %d", packetsW)
	}
	if dropped != 0 {
		t.Errorf("expected 0 dropped packets, got %d", dropped)
	}
	if bytesW == 0 {
		t.Errorf("expected >0 bytes written")
	}

	// Verify compressed file on disk
	currentDate := time.Now().UTC().Format("2006-01-02")
	expectedFile := filepath.Join(tempDir, fmt.Sprintf("massive_ws_%s.jsonl.zst", currentDate))

	f, err := os.Open(expectedFile)
	if err != nil {
		t.Fatalf("failed to open generated spool file %s: %v", expectedFile, err)
	}
	defer f.Close()

	decoder, err := zstd.NewReader(f)
	if err != nil {
		t.Fatalf("failed to create zstd reader: %v", err)
	}
	defer decoder.Close()

	var decompressedLines []string
	scanner := bufio.NewScanner(decoder)
	for scanner.Scan() {
		line := scanner.Text()
		if len(strings.TrimSpace(line)) > 0 {
			decompressedLines = append(decompressedLines, line)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("decompression scan error: %v", err)
	}

	if len(decompressedLines) != 5 {
		t.Fatalf("expected 5 decompressed lines, got %d: %v", len(decompressedLines), decompressedLines)
	}

	for i, expected := range testFrames {
		if decompressedLines[i] != string(expected) {
			t.Errorf("line %d mismatch: expected %q, got %q", i, string(expected), decompressedLines[i])
		}
	}
}

func TestRawSpoolerNonBlockingDrop(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tickhub_spool_drop_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Small buffer size = 2, do NOT call Start() so channel does not drain
	spooler, err := NewRawSpooler(RawSpoolerConfig{
		Dir:        tempDir,
		BufferSize: 2,
	})
	if err != nil {
		t.Fatalf("NewRawSpooler failed: %v", err)
	}

	pkt := []byte(`[{"ev":"Q","sym":"NVDA","bp":125.0,"ap":125.1}]`)
	if !spooler.Record(time.Now().UnixNano(), pkt) {
		t.Fatal("expected first packet to enqueue")
	}
	if !spooler.Record(time.Now().UnixNano(), pkt) {
		t.Fatal("expected second packet to enqueue")
	}

	// 3rd packet must drop non-blockingly without hanging
	droppedRecorded := spooler.Record(time.Now().UnixNano(), pkt)
	if droppedRecorded {
		t.Fatal("expected 3rd packet to drop due to full channel buffer")
	}

	_, _, dropped := spooler.Stats()
	if dropped != 1 {
		t.Fatalf("expected dropped count 1, got %d", dropped)
	}
}

func TestRawSpoolerShutdownDrain(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tickhub_spool_drain_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	spooler, err := NewRawSpooler(RawSpoolerConfig{
		Dir:        tempDir,
		BufferSize: 1000,
	})
	if err != nil {
		t.Fatalf("NewRawSpooler failed: %v", err)
	}

	ctx := context.Background()
	spooler.Start(ctx)

	const nPackets = 200
	for i := 0; i < nPackets; i++ {
		msg := fmt.Sprintf(`[{"ev":"T","sym":"AAPL","p":%d.5,"s":10}]`, i)
		if !spooler.Record(time.Now().UnixNano(), []byte(msg)) {
			t.Fatalf("failed to record packet %d", i)
		}
	}

	// Immediate close must drain all 200 packets
	if err := spooler.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	_, packetsW, dropped := spooler.Stats()
	if packetsW != nPackets {
		t.Fatalf("expected %d packets written on drain, got %d", nPackets, packetsW)
	}
	if dropped != 0 {
		t.Fatalf("expected 0 dropped packets, got %d", dropped)
	}

	currentDate := time.Now().UTC().Format("2006-01-02")
	expectedFile := filepath.Join(tempDir, fmt.Sprintf("massive_ws_%s.jsonl.zst", currentDate))

	f, err := os.Open(expectedFile)
	if err != nil {
		t.Fatalf("failed to open spool file: %v", err)
	}
	defer f.Close()

	dec, err := zstd.NewReader(f)
	if err != nil {
		t.Fatalf("zstd.NewReader failed: %v", err)
	}
	defer dec.Close()

	data, err := io.ReadAll(dec)
	if err != nil {
		t.Fatalf("io.ReadAll failed: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != nPackets {
		t.Fatalf("expected %d lines in file, got %d", nPackets, len(lines))
	}
}

func TestMassiveWSClientWithSpoolerIntegration(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tickhub_client_spool_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	spooler, err := NewRawSpooler(RawSpoolerConfig{
		Dir:        tempDir,
		BufferSize: 1024,
	})
	if err != nil {
		t.Fatalf("NewRawSpooler failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	spooler.Start(ctx)
	defer spooler.Close()

	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()

		// 1. Read Auth
		_, _, _ = c.ReadMessage()
		_ = c.WriteJSON([]RawMassiveEvent{
			{Ev: "status", Status: "auth_success", Msg: "authenticated"},
		})

		// 2. Read Subscribe
		_, _, _ = c.ReadMessage()

		// 3. Emit test tick stream (2 separate WebSocket frames)
		_ = c.WriteJSON([]RawMassiveEvent{
			{Ev: "Q", Sym: "MSFT", Bp: 400.1, Ap: 400.2, Bs: 10, As: 10, T: 1778074260000},
		})
		_ = c.WriteJSON([]RawMassiveEvent{
			{Ev: "T", Sym: "MSFT", P: 400.15, S: 25, T: 1778074260050},
		})

		time.Sleep(100 * time.Millisecond)
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	client := NewMassiveWSClient(wsURL, "TEST_API_KEY", []string{"MSFT"}, 1024)
	client.SetRawSpooler(spooler)

	if err := client.Start(ctx); err != nil {
		t.Fatalf("client.Start failed: %v", err)
	}
	defer client.Close()

	// Drain 2 ticks from client
	ticksChan := client.Ticks()
	for i := 0; i < 2; i++ {
		select {
		case <-ticksChan:
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for tick %d", i+1)
		}
	}

	// Allow spooler to write
	time.Sleep(100 * time.Millisecond)

	_, packetsW, dropped := spooler.Stats()
	if packetsW < 2 {
		t.Errorf("expected at least 2 packets recorded by spooler, got %d", packetsW)
	}
	if dropped != 0 {
		t.Errorf("expected 0 drops, got %d", dropped)
	}
}
