package feed

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
)

const (
	DefaultRawSpoolBufferSize = 65536
	DefaultRawSpoolFlushTick  = 1 * time.Second
	DefaultRawSpoolBufferSizeB = 1024 * 1024 // 1 MB bufio buffer
)

// RawPacket represents an incoming raw wire WebSocket frame with local reception timestamp.
type RawPacket struct {
	RecvNS int64
	Data   []byte
}

// RawSpoolerConfig configures the asynchronous raw market data feed spooler.
type RawSpoolerConfig struct {
	Dir        string        // Destination directory for daily partitioned .jsonl.zst files
	BufferSize int           // Capacity of bounded ring channel (default: 65,536)
	FlushTick  time.Duration // Interval to flush compression buffers to disk (default: 1s)
}

// RawSpooler asynchronously compresses and persists raw wire WebSocket frames to daily .jsonl.zst files.
type RawSpooler struct {
	dir        string
	packetChan chan RawPacket
	stopChan   chan struct{}
	closeOnce  sync.Once
	wg         sync.WaitGroup
	flushTick  time.Duration

	// Atomic telemetry metrics
	bytesWritten   atomic.Uint64
	packetsWritten atomic.Uint64
	droppedFrames  atomic.Uint64

	// Internal state accessed exclusively by single writer goroutine
	currentDate string
	currentFile *os.File
	bufWriter   *bufio.Writer
	enc         *zstd.Encoder
}

// NewRawSpooler initializes a new RawSpooler instance.
func NewRawSpooler(cfg RawSpoolerConfig) (*RawSpooler, error) {
	if cfg.Dir == "" {
		return nil, fmt.Errorf("raw spooler directory cannot be empty")
	}
	bufSize := cfg.BufferSize
	if bufSize <= 0 {
		bufSize = DefaultRawSpoolBufferSize
	}
	flushTick := cfg.FlushTick
	if flushTick <= 0 {
		flushTick = DefaultRawSpoolFlushTick
	}

	return &RawSpooler{
		dir:        cfg.Dir,
		packetChan: make(chan RawPacket, bufSize),
		stopChan:   make(chan struct{}),
		flushTick:  flushTick,
	}, nil
}

// Record non-blockingly enqueues a raw frame. If the queue is saturated, it drops the frame and increments droppedFrames.
func (s *RawSpooler) Record(recvNS int64, data []byte) bool {
	if s == nil {
		return false
	}
	select {
	case s.packetChan <- RawPacket{RecvNS: recvNS, Data: data}:
		return true
	default:
		s.droppedFrames.Add(1)
		return false
	}
}

// Stats returns atomic telemetry counters for bytes, packets, and dropped frames.
func (s *RawSpooler) Stats() (bytesWritten uint64, packetsWritten uint64, droppedFrames uint64) {
	if s == nil {
		return 0, 0, 0
	}
	return s.bytesWritten.Load(), s.packetsWritten.Load(), s.droppedFrames.Load()
}

// SpoolStats satisfies the metrics.RawSpoolerStats interface.
func (s *RawSpooler) SpoolStats() (bytes uint64, packets uint64, dropped uint64) {
	return s.Stats()
}

// Start launches the background worker goroutine.
func (s *RawSpooler) Start(ctx context.Context) {
	s.wg.Add(1)
	go s.run(ctx)
}

func (s *RawSpooler) run(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(s.flushTick)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopChan:
			s.drain()
			s.closeCurrentFile()
			return
		case <-ctx.Done():
			s.drain()
			s.closeCurrentFile()
			return
		case pkt := <-s.packetChan:
			s.writePacket(pkt)
		case <-ticker.C:
			s.flush()
		}
	}
}

func (s *RawSpooler) drain() {
	for {
		select {
		case pkt := <-s.packetChan:
			s.writePacket(pkt)
		default:
			s.flush()
			return
		}
	}
}

func (s *RawSpooler) writePacket(pkt RawPacket) {
	currentDate := time.Now().UTC().Format("2006-01-02")
	if currentDate != s.currentDate || s.currentFile == nil {
		if err := s.rotate(currentDate); err != nil {
			log.Printf("[RAW-SPOOL] File rotation failed for date %s: %v", currentDate, err)
			return
		}
	}

	data := bytes.TrimRight(pkt.Data, "\r\n")
	if len(data) == 0 {
		return
	}

	n, err := s.enc.Write(data)
	if err != nil {
		log.Printf("[RAW-SPOOL] Failed writing to zstd encoder: %v", err)
		return
	}
	_, err = s.enc.Write([]byte("\n"))
	if err != nil {
		log.Printf("[RAW-SPOOL] Failed writing newline to zstd encoder: %v", err)
		return
	}

	s.bytesWritten.Add(uint64(n + 1))
	s.packetsWritten.Add(1)
}

func (s *RawSpooler) rotate(newDate string) error {
	s.closeCurrentFile()

	if err := os.MkdirAll(s.dir, 0755); err != nil {
		return fmt.Errorf("create spool dir %s: %w", s.dir, err)
	}

	fileName := fmt.Sprintf("massive_ws_%s.jsonl.zst", newDate)
	filePath := filepath.Join(s.dir, fileName)

	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open spool file %s: %w", filePath, err)
	}

	bufWriter := bufio.NewWriterSize(f, DefaultRawSpoolBufferSizeB)

	enc, err := zstd.NewWriter(bufWriter, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("create zstd encoder: %w", err)
	}

	s.currentFile = f
	s.bufWriter = bufWriter
	s.enc = enc
	s.currentDate = newDate

	log.Printf("[RAW-SPOOL] Active spool file opened: %s", filePath)
	return nil
}

func (s *RawSpooler) flush() {
	if s.enc != nil {
		_ = s.enc.Flush()
	}
	if s.bufWriter != nil {
		_ = s.bufWriter.Flush()
	}
}

func (s *RawSpooler) closeCurrentFile() {
	if s.enc != nil {
		_ = s.enc.Close()
		s.enc = nil
	}
	if s.bufWriter != nil {
		_ = s.bufWriter.Flush()
		s.bufWriter = nil
	}
	if s.currentFile != nil {
		_ = s.currentFile.Close()
		s.currentFile = nil
	}
}

// Close gracefully closes the spooler, draining buffered frames within a 5-second deadline.
func (s *RawSpooler) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		close(s.stopChan)
		done := make(chan struct{})
		go func() {
			s.wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			log.Printf("[RAW-SPOOL] Graceful flush exceeded 5s deadline. Closing immediately.")
		}
	})
	return nil
}
