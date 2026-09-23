package shm

import (
	"testing"
	"unsafe"
)

func TestStructSizesAndAlignments(t *testing.T) {
	if sz := unsafe.Sizeof(GlobalHeader{}); sz != 1024 {
		t.Fatalf("expected GlobalHeader size 1024, got %d", sz)
	}

	// Verify Producer cache-line alignment at offset 64 (0x0040)
	var gh GlobalHeader
	if off := unsafe.Offsetof(gh.AnchorPublishLatencyNS); off != 64 {
		t.Fatalf("expected AnchorPublishLatencyNS at offset 64, got %d", off)
	}

	// Verify Consumer cache-line alignment at offset 128 (0x0080)
	if off := unsafe.Offsetof(gh.LastReadAnchorNS); off != 128 {
		t.Fatalf("expected LastReadAnchorNS at offset 128, got %d", off)
	}

	if sz := unsafe.Sizeof(SymbolSnapshot{}); sz != 128 {
		t.Fatalf("expected SymbolSnapshot size 128, got %d", sz)
	}

	if sz := unsafe.Sizeof(FrameHeader{}); sz != 64 {
		t.Fatalf("expected FrameHeader size 64, got %d", sz)
	}

	if sz := unsafe.Sizeof(SymbolDirectoryEntry{}); sz != 16 {
		t.Fatalf("expected SymbolDirectoryEntry size 16, got %d", sz)
	}

	if sz := unsafe.Sizeof(PhaseInfo{}); sz != 32 {
		t.Fatalf("expected PhaseInfo size 32, got %d", sz)
	}
}
