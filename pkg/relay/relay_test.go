package relay

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"
	"unsafe"

	"github.com/benwulfe-fb/tickhub/pkg/shm"
)

func TestProtocolEncodingDecoding(t *testing.T) {
	// 1. Handshake
	h := HandshakeHeader{
		Magic:             RelayMagic,
		Version:           RelayVersion,
		ABIVersion:        1,
		Mode:              1,
		CadenceIntervalNS: 1_000_000_000,
		MaxFrames:         64,
		TotalSymbols:      2,
		NumPhases:         1,
	}
	phases := []shm.PhaseInfo{
		{
			PhaseID:          1,
			OffsetMS:         0,
			NumSymbols:       2,
			NumFeatures:      5,
			FrameStrideBytes: 192,
			RingOffsetBytes:  65536,
		},
	}
	symbols := []string{"AAPL", "NVDA"}
	customYAML := []byte("features:\n  - f1\n  - f2\n")

	buf := EncodeHandshake(h, phases, symbols, customYAML)
	hDec, phasesDec, symbolsDec, configYAMLDec, err := DecodeHandshake(buf)
	if err != nil {
		t.Fatalf("DecodeHandshake error: %v", err)
	}
	if string(configYAMLDec) != string(customYAML) {
		t.Fatalf("configYAML mismatch: expected '%s', got '%s'", string(customYAML), string(configYAMLDec))
	}
	if hDec.Magic != RelayMagic || hDec.Version != RelayVersion || hDec.MaxFrames != 64 || hDec.Mode != 1 {
		t.Fatalf("Handshake header mismatch: %+v", hDec)
	}
	if len(phasesDec) != 1 || phasesDec[0].FrameStrideBytes != 192 {
		t.Fatalf("Phase mismatch: %+v", phasesDec)
	}
	if len(symbolsDec) != 2 || symbolsDec[0] != "AAPL" || symbolsDec[1] != "NVDA" {
		t.Fatalf("Symbols mismatch: %+v", symbolsDec)
	}

	// 2. Snapshot
	snap := &shm.SymbolSnapshot{
		SeqLockSeq:      2,
		SIPTimestampNS:  123456789,
		RecvTimestampNS: 123456799,
		BidPx:           150.25,
		AskPx:           150.30,
		BidSz:           100,
		AskSz:           200,
		LastTradePx:     150.27,
		LastTradeSz:     50,
		Midprice:        150.275,
		Spread:          0.05,
	}
	snapBuf := EncodeSnapshot(1, snap)
	symIdx, snapDec, err := DecodeSnapshot(snapBuf)
	if err != nil {
		t.Fatalf("DecodeSnapshot error: %v", err)
	}
	if symIdx != 1 {
		t.Fatalf("Expected symIdx 1, got %d", symIdx)
	}
	if snapDec.BidPx != 150.25 || snapDec.AskPx != 150.30 || snapDec.Midprice != 150.275 {
		t.Fatalf("Snapshot fields mismatch: %+v", snapDec)
	}

	// 3. FrameSlot
	rawBytes := []byte("0123456789abcdef0123456789abcdef")
	slotBuf := EncodeFrameSlot(0, 4, 1_000_000_000, rawBytes)
	pIdx, sIdx, anchorNS, rawDec, err := DecodeFrameSlot(slotBuf)
	if err != nil {
		t.Fatalf("DecodeFrameSlot error: %v", err)
	}
	if pIdx != 0 || sIdx != 4 || anchorNS != 1_000_000_000 {
		t.Fatalf("FrameSlot header mismatch: pIdx=%d, sIdx=%d, anchor=%d", pIdx, sIdx, anchorNS)
	}
	if !bytes.Equal(rawDec, rawBytes) {
		t.Fatalf("RawBytes mismatch")
	}

	// 4. AnchorCommit
	commitBuf := EncodeAnchorCommit(2_000_000_000, 3)
	commitAnchor, count, err := DecodeAnchorCommit(commitBuf)
	if err != nil {
		t.Fatalf("DecodeAnchorCommit error: %v", err)
	}
	if commitAnchor != 2_000_000_000 || count != 3 {
		t.Fatalf("AnchorCommit mismatch: anchor=%d, count=%d", commitAnchor, count)
	}
}

func TestRelayLoopbackReplication(t *testing.T) {
	primarySHM := "test_relay_primary"
	replicaSHM := "test_relay_replica"

	symbols := []string{"AAPL", "MSFT"}
	phases := []shm.PhaseConfig{
		{
			ID:       1,
			Name:     "phase_0ms",
			OffsetMS: 0,
			Symbols:  symbols,
		},
	}
	features := []string{"ret1s", "ret5s", "ret15s", "vol1s", "spread"}

	primCfg := shm.Config{
		Name:            primarySHM,
		MaxFrames:       64,
		CadenceInterval: time.Second,
		UniqueSymbols:   symbols,
		Features:        features,
		Phases:          phases,
		Permissions:     0666,
		UnlinkOnExit:    true,
		Mode:            shm.ModeHistoricalReplay,
	}

	primProd, err := shm.CreateProducer(primCfg)
	if err != nil {
		t.Fatalf("Create primary producer: %v", err)
	}
	defer primProd.Close()
	primProd.SetStatus(shm.StatusRunning)

	// Populate some data in primary
	var anchor1 int64 = 1_000_000_000
	snap1 := &shm.SymbolSnapshot{
		SIPTimestampNS: anchor1 - 500_000_000,
		BidPx:          100.5,
		AskPx:          100.6,
		BidSz:          10,
		AskSz:          20,
		Midprice:       100.55,
		Spread:         0.10,
	}
	primProd.WriteSnapshot(0, snap1)

	metrics1 := []float64{0.001, 0.002, 0.003, 100.0, 0.10}
	if err := primProd.CommitSymbolMetrics(0, 0, anchor1, metrics1); err != nil {
		t.Fatalf("Commit metrics: %v", err)
	}
	metrics2 := []float64{0.002, 0.004, 0.006, 200.0, 0.15}
	if err := primProd.CommitSymbolMetrics(0, 1, anchor1, metrics2); err != nil {
		t.Fatalf("Commit metrics: %v", err)
	}
	primProd.CommitFrameFinalize(anchor1)

	// Start RelayServer
	srv, err := NewServerWithSegment(ServerConfig{
		FromStart:    true,
		PollInterval: 50 * time.Microsecond,
	}, primProd.Segment())
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	srv.listener = l
	go srv.Serve(l)
	defer srv.Close()

	// Start RelayClient
	cli := NewClient(ClientConfig{
		ServerAddr:   l.Addr().String(),
		SHMName:      replicaSHM,
		Permissions:  0666,
		UnlinkOnExit: true,
		Timeout:      2 * time.Second,
	})
	defer cli.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = cli.ConnectAndReplicate(ctx)
	}()

	// Wait for replication of anchor1
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cli.ReplicatedAnchors() >= 1 && cli.LastReplicatedAnchorNS() == anchor1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if cli.ReplicatedAnchors() < 1 {
		t.Fatalf("Client failed to replicate anchor1, replicated=%d", cli.ReplicatedAnchors())
	}

	// Now commit anchor2 in primary while client is connected
	var anchor2 int64 = 2_000_000_000
	snap2 := &shm.SymbolSnapshot{
		SIPTimestampNS: anchor2 - 300_000_000,
		BidPx:          101.0,
		AskPx:          101.1,
		BidSz:          50,
		AskSz:          60,
		Midprice:       101.05,
		Spread:         0.10,
	}
	primProd.WriteSnapshot(1, snap2)
	if err := primProd.CommitSymbolMetrics(0, 0, anchor2, metrics1); err != nil {
		t.Fatalf("Commit anchor2 metrics: %v", err)
	}
	if err := primProd.CommitSymbolMetrics(0, 1, anchor2, metrics2); err != nil {
		t.Fatalf("Commit anchor2 metrics: %v", err)
	}
	primProd.CommitFrameFinalize(anchor2)

	// Wait for replication of anchor2
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cli.ReplicatedAnchors() >= 2 && cli.LastReplicatedAnchorNS() == anchor2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if cli.LastReplicatedAnchorNS() != anchor2 {
		t.Fatalf("Expected replicated anchor %d, got %d", anchor2, cli.LastReplicatedAnchorNS())
	}

	// Verify Bit-Identical raw slot bytes
	slot1 := uint32((anchor1 / 1_000_000_000) & 63)
	slot2 := uint32((anchor2 / 1_000_000_000) & 63)

	primSlot1, err := primProd.ReadRawSlot(0, slot1)
	if err != nil {
		t.Fatalf("Read primary slot1: %v", err)
	}
	replSlot1, err := cli.Producer().ReadRawSlot(0, slot1)
	if err != nil {
		t.Fatalf("Read replica slot1: %v", err)
	}
	if !bytes.Equal(primSlot1, replSlot1) {
		t.Fatalf("Slot1 mismatch between primary and replica!")
	}

	primSlot2, err := primProd.ReadRawSlot(0, slot2)
	if err != nil {
		t.Fatalf("Read primary slot2: %v", err)
	}
	replSlot2, err := cli.Producer().ReadRawSlot(0, slot2)
	if err != nil {
		t.Fatalf("Read replica slot2: %v", err)
	}
	if !bytes.Equal(primSlot2, replSlot2) {
		t.Fatalf("Slot2 mismatch between primary and replica!")
	}

	// Verify Bit-Identical Snapshot
	replSnap0 := (*shm.SymbolSnapshot)(unsafe.Pointer(&cli.Producer().Bytes()[shm.SnapshotOffset]))
	if replSnap0.BidPx != snap1.BidPx || replSnap0.AskPx != snap1.AskPx || replSnap0.Midprice != snap1.Midprice {
		t.Fatalf("Replica snapshot0 mismatch: %+v", replSnap0)
	}
	replSnap1 := (*shm.SymbolSnapshot)(unsafe.Pointer(&cli.Producer().Bytes()[shm.SnapshotOffset+128]))
	if replSnap1.BidPx != snap2.BidPx || replSnap1.AskPx != snap2.AskPx || replSnap1.Midprice != snap2.Midprice {
		t.Fatalf("Replica snapshot1 mismatch: %+v", replSnap1)
	}
}
