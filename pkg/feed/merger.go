package feed

import (
	"container/heap"
)

type heapItem struct {
	reader TickReader
	tick   Tick
}

type tickMinHeap []*heapItem

func (h tickMinHeap) Len() int { return len(h) }

func (h tickMinHeap) Less(i, j int) bool {
	ti, tj := h[i].tick, h[j].tick
	if ti.SIPTimestampNS != tj.SIPTimestampNS {
		return ti.SIPTimestampNS < tj.SIPTimestampNS
	}
	// Tie-break 1: Quotes strictly precede Trades
	if ti.Type != tj.Type {
		return ti.Type < tj.Type
	}
	// Tie-break 2: Deterministic Symbol ASCII ordering
	return ti.Symbol < tj.Symbol
}

func (h tickMinHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *tickMinHeap) Push(x any) {
	*h = append(*h, x.(*heapItem))
}

func (h *tickMinHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// Merger combines N TickReaders into a single, strictly chronological tick stream.
type Merger struct {
	h       tickMinHeap
	current Tick
	err     error
	readers []TickReader
}

// NewMerger creates and initializes a K-way min-heap tick merger from active readers.
func NewMerger(readers []TickReader) *Merger {
	m := &Merger{
		h:       make(tickMinHeap, 0, len(readers)),
		readers: readers,
	}

	for _, r := range readers {
		if r.Next() {
			item := &heapItem{
				reader: r,
				tick:   r.Tick(),
			}
			m.h = append(m.h, item)
		} else if err := r.Err(); err != nil && m.err == nil {
			m.err = err
		}
	}

	heap.Init(&m.h)
	return m
}

// Next advances the merger to the earliest chronological tick.
func (m *Merger) Next() bool {
	if len(m.h) == 0 {
		return false
	}

	top := m.h[0]
	m.current = top.tick

	// Advance the reader that produced the top tick
	if top.reader.Next() {
		top.tick = top.reader.Tick()
		heap.Fix(&m.h, 0)
	} else {
		if err := top.reader.Err(); err != nil && m.err == nil {
			m.err = err
		}
		_ = top.reader.Close()
		heap.Pop(&m.h)
	}

	return true
}

func (m *Merger) Tick() Tick {
	return m.current
}

func (m *Merger) Err() error {
	return m.err
}

func (m *Merger) Close() error {
	var firstErr error
	for _, r := range m.readers {
		if err := r.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	m.h = nil
	return firstErr
}
