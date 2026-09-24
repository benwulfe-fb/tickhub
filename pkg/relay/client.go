package relay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benwulfe-fb/tickhub/pkg/shm"
)

// ClientConfig configures the relay TCP replication client.
type ClientConfig struct {
	ServerAddr   string
	SHMName      string
	Permissions  uint32
	UnlinkOnExit bool
	Timeout      time.Duration
}

// Client connects to a RelayServer, creates a replica SHM segment, and writes replicated frames and snapshots.
type Client struct {
	cfg          ClientConfig
	mu           sync.Mutex
	conn         net.Conn
	producer     *shm.Producer
	closed       atomic.Bool
	anchorsCount atomic.Int64
	lastAnchor   atomic.Int64
	reconnects   atomic.Int64
	seqGaps      atomic.Int64
}

// NewClient creates a new relay client.
func NewClient(cfg ClientConfig) *Client {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.SHMName == "" {
		cfg.SHMName = "tickhub_live"
	}
	return &Client{
		cfg: cfg,
	}
}

// ConnectAndReplicate connects to the relay server and runs the replication pump with automatic reconnect.
func (c *Client) ConnectAndReplicate(ctx context.Context) error {
	dialer := net.Dialer{Timeout: c.cfg.Timeout}
	retryDelay := 500 * time.Millisecond
	maxDelay := 5 * time.Second

	for {
		if c.closed.Load() || ctx.Err() != nil {
			return nil
		}

		conn, err := dialer.DialContext(ctx, "tcp", c.cfg.ServerAddr)
		if err != nil {
			if c.closed.Load() || ctx.Err() != nil {
				return nil
			}
			c.reconnects.Add(1)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(retryDelay):
				if retryDelay < maxDelay {
					retryDelay *= 2
				}
				continue
			}
		}

		retryDelay = 500 * time.Millisecond
		c.mu.Lock()
		c.conn = conn
		c.mu.Unlock()

		err = c.runStream(ctx, conn)

		c.mu.Lock()
		if c.conn != nil {
			_ = c.conn.Close()
			c.conn = nil
		}
		c.mu.Unlock()

		if c.closed.Load() || ctx.Err() != nil {
			return nil
		}

		if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
			return nil
		}

		c.reconnects.Add(1)
		log.Printf("[RELAY-CLI] Stream disconnected (%v), reconnecting (reconnects=%d)...", err, c.reconnects.Load())
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(retryDelay):
			if retryDelay < maxDelay {
				retryDelay *= 2
			}
		}
	}
}

func (c *Client) runStream(ctx context.Context, conn net.Conn) error {
	// 1. Receive Handshake
	msgType, _, payload, err := ReadPacket(conn)
	if err != nil {
		return fmt.Errorf("read handshake: %w", err)
	}
	if msgType != MsgTypeHandshake {
		return fmt.Errorf("expected handshake (0x%02X), got 0x%02X", MsgTypeHandshake, msgType)
	}

	h, phases, symbols, err := DecodeHandshake(payload)
	if err != nil {
		return fmt.Errorf("decode handshake: %w", err)
	}

	// 2. Create replica SHM Producer if not yet allocated
	if c.producer == nil {
		shmCfg := createReplicaConfig(c.cfg, h, phases, symbols)
		prod, err := shm.CreateProducer(shmCfg)
		if err != nil {
			return fmt.Errorf("create replica producer %s: %w", c.cfg.SHMName, err)
		}
		c.producer = prod
		c.producer.SetStatus(shm.StatusRunning)
		log.Printf("[RELAY-CLI] Replicating into /dev/shm/%s (max_frames=%d, phases=%d, symbols=%d)",
			c.cfg.SHMName, h.MaxFrames, len(phases), len(symbols))
	}

	// 3. Streaming replication loop
	var pendingSnapshots uint32 = 0
	var lastPacketSeq uint64 = 0

	for {
		if c.closed.Load() || ctx.Err() != nil {
			return nil
		}

		mType, seq, msgPayload, err := ReadPacket(conn)
		if err != nil {
			return err
		}

		if lastPacketSeq > 0 && seq != lastPacketSeq+1 {
			c.seqGaps.Add(int64(seq - lastPacketSeq - 1))
		}
		lastPacketSeq = seq

		switch mType {
		case MsgTypeSnapshot:
			symbolIdx, snap, err := DecodeSnapshot(msgPayload)
			if err != nil {
				return fmt.Errorf("decode snapshot: %w", err)
			}
			c.producer.WriteSnapshot(int(symbolIdx), snap)
			pendingSnapshots++

		case MsgTypeFrameSlot:
			phaseIdx, slotIdx, _, rawSlotBytes, err := DecodeFrameSlot(msgPayload)
			if err != nil {
				return fmt.Errorf("decode frame slot: %w", err)
			}
			if err := c.producer.WriteRawSlot(int(phaseIdx), slotIdx, rawSlotBytes); err != nil {
				return fmt.Errorf("write raw slot (p=%d, slot=%d): %w", phaseIdx, slotIdx, err)
			}

		case MsgTypeAnchor:
			anchorNS, numSnapshots, err := DecodeAnchorCommit(msgPayload)
			if err != nil {
				return fmt.Errorf("decode anchor commit: %w", err)
			}
			if pendingSnapshots != numSnapshots {
				log.Printf("[RELAY-CLI] Warning: snapshot count mismatch @ %d (applied %d, expected %d)",
					anchorNS, pendingSnapshots, numSnapshots)
			}
			appliedSnapshots := pendingSnapshots
			pendingSnapshots = 0

			tNow := time.Now().UnixNano()
			latencyUS := int64(0)
			if tNow > anchorNS {
				latencyUS = (tNow - anchorNS) / 1000
			}

			c.producer.CommitFrameFinalize(anchorNS)
			c.anchorsCount.Add(1)
			c.lastAnchor.Store(anchorNS)

			log.Printf("[RELAY-CLI] Replicated anchor %d to /dev/shm/%s (%d snapshots, latency %dµs, gaps=%d, reconnects=%d)",
				anchorNS, c.cfg.SHMName, appliedSnapshots, latencyUS, c.seqGaps.Load(), c.reconnects.Load())

		default:
			log.Printf("[RELAY-CLI] Unrecognized message type 0x%02X, skipping", mType)
		}
	}
}

// Producer returns the underlying replica producer, if allocated.
func (c *Client) Producer() *shm.Producer {
	return c.producer
}

// ReplicatedAnchors returns the number of committed anchors replicated so far.
func (c *Client) ReplicatedAnchors() int64 {
	return c.anchorsCount.Load()
}

// LastReplicatedAnchorNS returns the most recent committed anchor timestamp.
func (c *Client) LastReplicatedAnchorNS() int64 {
	return c.lastAnchor.Load()
}

// Reconnects returns the number of reconnections performed.
func (c *Client) Reconnects() int64 {
	return c.reconnects.Load()
}

// SeqGaps returns the number of packet sequence gaps detected.
func (c *Client) SeqGaps() int64 {
	return c.seqGaps.Load()
}

// Close disconnects client and releases replica SHM.
func (c *Client) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	c.mu.Lock()
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
	c.mu.Unlock()
	if c.producer != nil {
		return c.producer.Close()
	}
	return nil
}

func createReplicaConfig(cfg ClientConfig, h *HandshakeHeader, phases []shm.PhaseInfo, symbols []string) shm.Config {
	phaseConfigs := make([]shm.PhaseConfig, len(phases))
	for i, p := range phases {
		syms := make([]string, p.NumSymbols)
		for sIdx := 0; sIdx < int(p.NumSymbols); sIdx++ {
			if sIdx < len(symbols) {
				syms[sIdx] = symbols[sIdx]
			} else {
				syms[sIdx] = fmt.Sprintf("SYM%d", sIdx)
			}
		}
		phaseConfigs[i] = shm.PhaseConfig{
			ID:       p.PhaseID,
			Name:     fmt.Sprintf("phase_%d", p.PhaseID),
			OffsetMS: p.OffsetMS,
			Symbols:  syms,
		}
	}

	featCount := 0
	if len(phases) > 0 {
		featCount = int(phases[0].NumFeatures)
	}
	dummyFeatures := make([]string, featCount)
	for i := 0; i < featCount; i++ {
		dummyFeatures[i] = fmt.Sprintf("f%d", i)
	}

	perm := cfg.Permissions
	if perm == 0 {
		perm = 0666
	}

	return shm.Config{
		Name:            cfg.SHMName,
		MaxFrames:       h.MaxFrames,
		CadenceInterval: time.Duration(h.CadenceIntervalNS),
		UniqueSymbols:   symbols,
		Features:        dummyFeatures,
		Phases:          phaseConfigs,
		Permissions:     perm,
		UnlinkOnExit:    cfg.UnlinkOnExit,
		Mode:            h.Mode,
	}
}
