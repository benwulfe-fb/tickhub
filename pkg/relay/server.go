package relay

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/benwulfe-fb/tickhub/pkg/shm"
)

// ServerConfig configures the relay TCP streaming server.
type ServerConfig struct {
	SHMName      string
	ListenAddr   string
	FromStart    bool
	PollInterval time.Duration
}

// Server attaches to a source SHM segment and streams committed frames and snapshots to TCP clients.
type Server struct {
	cfg      ServerConfig
	segment  *shm.Segment
	listener net.Listener
	closed   atomic.Bool
	quit     chan struct{}
	wg       sync.WaitGroup
	ownsSeg  bool
}

// NewServer creates a relay server attached to an existing SHM segment by name.
func NewServer(cfg ServerConfig) (*Server, error) {
	seg, err := shm.AttachSegment(cfg.SHMName, true)
	if err != nil {
		return nil, fmt.Errorf("relay-server attach to %s: %w", cfg.SHMName, err)
	}
	s, err := NewServerWithSegment(cfg, seg)
	if err != nil {
		seg.Close(false)
		return nil, err
	}
	s.ownsSeg = true
	return s, nil
}

// NewServerWithSegment creates a relay server using an already open segment.
func NewServerWithSegment(cfg ServerConfig, seg *shm.Segment) (*Server, error) {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 100 * time.Microsecond
	}
	return &Server{
		cfg:     cfg,
		segment: seg,
		quit:    make(chan struct{}),
	}, nil
}

// Start begins listening on TCP and accepts clients in background.
func (s *Server) Start() error {
	l, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.cfg.ListenAddr, err)
	}
	s.listener = l
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		_ = s.Serve(l)
	}()
	return nil
}

// Addr returns the listener address (useful when dynamic port ":0" is used).
func (s *Server) Addr() net.Addr {
	if s.listener != nil {
		return s.listener.Addr()
	}
	return nil
}

// Serve accepts client connections on l.
func (s *Server) Serve(l net.Listener) error {
	for {
		conn, err := l.Accept()
		if err != nil {
			if s.closed.Load() {
				return nil
			}
			return err
		}

		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			s.handleConn(c)
		}(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()

	hdr := s.segment.Header()
	numPhases := int(hdr.NumPhases)
	phases := make([]shm.PhaseInfo, numPhases)
	copy(phases, hdr.Phases[:numPhases])

	symbols := readDirectorySymbols(s.segment, int(hdr.TotalSymbols))

	h := HandshakeHeader{
		Magic:             RelayMagic,
		Version:           RelayVersion,
		ABIVersion:        hdr.Version,
		Mode:              hdr.Mode,
		CadenceIntervalNS: int64(hdr.CadenceInterval),
		MaxFrames:         hdr.MaxFrames,
		TotalSymbols:      hdr.TotalSymbols,
		NumPhases:         hdr.NumPhases,
	}

	handshakePayload := EncodeHandshake(h, phases, symbols)
	var seq uint64 = 1
	if err := WritePacket(conn, MsgTypeHandshake, seq, handshakePayload); err != nil {
		log.Printf("[RELAY-SRV] Handshake write error to %s: %v", conn.RemoteAddr(), err)
		return
	}
	seq++

	cadenceNS := int64(hdr.CadenceInterval)
	if cadenceNS <= 0 {
		cadenceNS = int64(time.Second)
	}

	// Wait for first anchor to be committed
	for {
		if s.closed.Load() {
			return
		}
		first := atomic.LoadInt64(&hdr.FirstAnchorNS)
		last := atomic.LoadInt64(&hdr.LastWrittenAnchorNS)
		if first > 0 || last > 0 {
			break
		}
		select {
		case <-s.quit:
			return
		case <-time.After(5 * time.Millisecond):
		}
	}

	firstAnchor := atomic.LoadInt64(&hdr.FirstAnchorNS)
	lastAnchor := atomic.LoadInt64(&hdr.LastWrittenAnchorNS)

	var nextAnchor int64
	if s.cfg.FromStart || hdr.Mode == shm.ModeHistoricalReplay {
		if firstAnchor > 0 {
			nextAnchor = firstAnchor
		} else {
			nextAnchor = lastAnchor
		}
	} else {
		nextAnchor = lastAnchor
	}

	lastSentSeq := make([]uint64, hdr.TotalSymbols)
	bw := bufio.NewWriterSize(conn, 64*1024)
	var scratchSnap shm.SymbolSnapshot
	isFirstClientAnchor := true

	pollTimer := time.NewTimer(s.cfg.PollInterval)
	defer pollTimer.Stop()

	for {
		if s.closed.Load() {
			return
		}

		currentLast := atomic.LoadInt64(&hdr.LastWrittenAnchorNS)
		status := atomic.LoadUint32(&hdr.Status)

		if nextAnchor <= currentLast && nextAnchor > 0 {
			// Check ring buffer overrun
			earliest := currentLast - int64(hdr.MaxFrames-2)*cadenceNS
			if nextAnchor < earliest && earliest > 0 {
				nextAnchor = earliest
			}

			// 1. Emit updated snapshots in [windowStart, nextAnchor) (or all baseline snapshots on client connection)
			var numSnapshots uint32 = 0
			windowStart := nextAnchor - cadenceNS
			for i := 0; i < int(hdr.TotalSymbols); i++ {
				readSnapshotSeqLock(s.segment, i, &scratchSnap)
				if scratchSnap.SeqLockSeq > 0 && scratchSnap.SeqLockSeq != lastSentSeq[i] {
					if isFirstClientAnchor ||
						(scratchSnap.SIPTimestampNS >= windowStart && scratchSnap.SIPTimestampNS < nextAnchor) {
						snapPayload := EncodeSnapshot(uint16(i), &scratchSnap)
						if err := WritePacket(bw, MsgTypeSnapshot, seq, snapPayload); err != nil {
							return
						}
						seq++
						lastSentSeq[i] = scratchSnap.SeqLockSeq
						numSnapshots++
					}
				}
			}
			isFirstClientAnchor = false

			// 2. Emit raw frame slot for each phase
			slot := uint32((nextAnchor / cadenceNS) & int64(hdr.MaxFrames-1))
			raw := s.segment.Bytes()
			for pIdx := 0; pIdx < numPhases; pIdx++ {
				pInfo := hdr.Phases[pIdx]
				offset := pInfo.RingOffsetBytes + uint64(slot)*pInfo.FrameStrideBytes
				slotBytes := raw[offset : offset+pInfo.FrameStrideBytes]
				slotPayload := EncodeFrameSlot(uint8(pIdx), slot, nextAnchor, slotBytes)
				if err := WritePacket(bw, MsgTypeFrameSlot, seq, slotPayload); err != nil {
					return
				}
				seq++
			}

			// 3. Emit anchor commit
			commitPayload := EncodeAnchorCommit(nextAnchor, numSnapshots)
			if err := WritePacket(bw, MsgTypeAnchor, seq, commitPayload); err != nil {
				return
			}
			seq++

			if err := bw.Flush(); err != nil {
				return
			}

			log.Printf("[RELAY-SRV] Streaming anchor %d to %s (%d snapshots, %d frame slots)",
				nextAnchor, conn.RemoteAddr(), numSnapshots, numPhases)

			nextAnchor += cadenceNS
			continue
		}

		if status == shm.StatusClosed {
			// Flush any final top-of-book snapshots before connection terminates
			for i := 0; i < int(hdr.TotalSymbols); i++ {
				readSnapshotSeqLock(s.segment, i, &scratchSnap)
				if scratchSnap.SeqLockSeq > 0 && scratchSnap.SeqLockSeq != lastSentSeq[i] {
					snapPayload := EncodeSnapshot(uint16(i), &scratchSnap)
					_ = WritePacket(bw, MsgTypeSnapshot, seq, snapPayload)
					seq++
					lastSentSeq[i] = scratchSnap.SeqLockSeq
				}
			}
			_ = bw.Flush()
			return
		}

		if !pollTimer.Stop() {
			select {
			case <-pollTimer.C:
			default:
			}
		}
		pollTimer.Reset(s.cfg.PollInterval)

		select {
		case <-s.quit:
			_ = bw.Flush()
			return
		case <-pollTimer.C:
		}
	}
}

// Close gracefully closes listener and connections.
func (s *Server) Close() error {
	if s.closed.Swap(true) {
		return nil
	}
	close(s.quit)
	var err error
	if s.listener != nil {
		err = s.listener.Close()
	}
	s.wg.Wait()
	if s.ownsSeg && s.segment != nil {
		_ = s.segment.Close(false)
	}
	return err
}

func readSnapshotSeqLock(seg *shm.Segment, symbolIdx int, dst *shm.SymbolSnapshot) {
	raw := seg.Bytes()
	offset := shm.SnapshotOffset + uintptr(symbolIdx)*128
	src := (*shm.SymbolSnapshot)(unsafe.Pointer(&raw[offset]))
	snapSize := int(unsafe.Sizeof(*dst))

	srcBytes := unsafe.Slice((*byte)(unsafe.Pointer(src)), snapSize)
	dstBytes := unsafe.Slice((*byte)(unsafe.Pointer(dst)), snapSize)

	const maxSpins = 1000
	for spin := 0; spin < maxSpins; spin++ {
		seq1 := atomic.LoadUint64(&src.SeqLockSeq)
		if seq1%2 != 0 {
			runtime.Gosched()
			continue
		}
		copy(dstBytes, srcBytes)
		seq2 := atomic.LoadUint64(&src.SeqLockSeq)
		if seq1 == seq2 {
			return
		}
		runtime.Gosched()
	}
	// Fallback under high contention / terminated producer: best-effort copy
	copy(dstBytes, srcBytes)
}

func readDirectorySymbols(seg *shm.Segment, totalSymbols int) []string {
	raw := seg.Bytes()
	syms := make([]string, totalSymbols)
	for i := 0; i < totalSymbols; i++ {
		entry := (*shm.SymbolDirectoryEntry)(unsafe.Pointer(&raw[shm.DirectoryOffset+uintptr(i*16)]))
		nameBytes := entry.Name[:]
		name := string(nameBytes)
		for j, b := range nameBytes {
			if b == 0 {
				name = string(nameBytes[:j])
				break
			}
		}
		syms[i] = name
	}
	return syms
}
