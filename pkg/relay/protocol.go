package relay

import (
	"encoding/binary"
	"errors"
	"io"
	"unsafe"

	"github.com/benwulfe-fb/tickhub/pkg/shm"
)

const (
	RelayMagic   uint32 = 0x5448524C // "THRL"
	RelayVersion uint32 = 1

	MsgTypeHandshake uint8 = 0x01
	MsgTypeSnapshot  uint8 = 0x02
	MsgTypeFrameSlot uint8 = 0x03
	MsgTypeAnchor    uint8 = 0x04
)

var (
	ErrInvalidMagic   = errors.New("invalid relay magic")
	ErrVersionMismatch = errors.New("relay protocol version mismatch")
	ErrCorruptMessage = errors.New("corrupt relay message")
)

// HandshakeHeader carries initialization parameters from server to client.
type HandshakeHeader struct {
	Magic             uint32
	Version           uint32
	ABIVersion        uint32
	Mode              uint32
	CadenceIntervalNS int64
	MaxFrames         uint32
	TotalSymbols      uint32
	NumPhases         uint32
}

// EncodeHandshake encodes HandshakeHeader, PhaseInfos, and Symbol names.
func EncodeHandshake(h HandshakeHeader, phases []shm.PhaseInfo, symbols []string) []byte {
	buf := make([]byte, 36+len(phases)*32+len(symbols)*16)
	binary.BigEndian.PutUint32(buf[0:4], h.Magic)
	binary.BigEndian.PutUint32(buf[4:8], h.Version)
	binary.BigEndian.PutUint32(buf[8:12], h.ABIVersion)
	binary.BigEndian.PutUint32(buf[12:16], h.Mode)
	binary.BigEndian.PutUint64(buf[16:24], uint64(h.CadenceIntervalNS))
	binary.BigEndian.PutUint32(buf[24:28], h.MaxFrames)
	binary.BigEndian.PutUint32(buf[28:32], h.TotalSymbols)
	binary.BigEndian.PutUint32(buf[32:36], h.NumPhases)

	offset := 36
	for _, p := range phases {
		binary.BigEndian.PutUint32(buf[offset:offset+4], p.PhaseID)
		binary.BigEndian.PutUint32(buf[offset+4:offset+8], p.OffsetMS)
		binary.BigEndian.PutUint32(buf[offset+8:offset+12], p.NumSymbols)
		binary.BigEndian.PutUint32(buf[offset+12:offset+16], p.NumFeatures)
		binary.BigEndian.PutUint64(buf[offset+16:offset+24], p.FrameStrideBytes)
		binary.BigEndian.PutUint64(buf[offset+24:offset+32], p.RingOffsetBytes)
		offset += 32
	}

	for _, s := range symbols {
		copy(buf[offset:offset+16], s)
		offset += 16
	}

	return buf
}

// DecodeHandshake decodes binary payload into HandshakeHeader, PhaseInfos, and Symbol names.
func DecodeHandshake(data []byte) (*HandshakeHeader, []shm.PhaseInfo, []string, error) {
	if len(data) < 36 {
		return nil, nil, nil, io.ErrUnexpectedEOF
	}

	h := &HandshakeHeader{
		Magic:             binary.BigEndian.Uint32(data[0:4]),
		Version:           binary.BigEndian.Uint32(data[4:8]),
		ABIVersion:        binary.BigEndian.Uint32(data[8:12]),
		Mode:              binary.BigEndian.Uint32(data[12:16]),
		CadenceIntervalNS: int64(binary.BigEndian.Uint64(data[16:24])),
		MaxFrames:         binary.BigEndian.Uint32(data[24:28]),
		TotalSymbols:      binary.BigEndian.Uint32(data[28:32]),
		NumPhases:         binary.BigEndian.Uint32(data[32:36]),
	}

	if h.Magic != RelayMagic {
		return nil, nil, nil, ErrInvalidMagic
	}
	if h.Version != RelayVersion {
		return nil, nil, nil, ErrVersionMismatch
	}

	expectedLen := 36 + int(h.NumPhases)*32 + int(h.TotalSymbols)*16
	if len(data) < expectedLen {
		return nil, nil, nil, io.ErrUnexpectedEOF
	}

	phases := make([]shm.PhaseInfo, h.NumPhases)
	offset := 36
	for i := 0; i < int(h.NumPhases); i++ {
		phases[i] = shm.PhaseInfo{
			PhaseID:          binary.BigEndian.Uint32(data[offset : offset+4]),
			OffsetMS:         binary.BigEndian.Uint32(data[offset+4 : offset+8]),
			NumSymbols:       binary.BigEndian.Uint32(data[offset+8 : offset+12]),
			NumFeatures:      binary.BigEndian.Uint32(data[offset+12 : offset+16]),
			FrameStrideBytes: binary.BigEndian.Uint64(data[offset+16 : offset+24]),
			RingOffsetBytes:  binary.BigEndian.Uint64(data[offset+24 : offset+32]),
		}
		offset += 32
	}

	symbols := make([]string, h.TotalSymbols)
	for i := 0; i < int(h.TotalSymbols); i++ {
		symRaw := data[offset : offset+16]
		symbols[i] = string(symRaw)
		// trim null bytes
		for j, b := range symRaw {
			if b == 0 {
				symbols[i] = string(symRaw[:j])
				break
			}
		}
		offset += 16
	}

	return h, phases, symbols, nil
}

// EncodeSnapshot encodes symbolIdx and SymbolSnapshot.
func EncodeSnapshot(symbolIdx uint16, snap *shm.SymbolSnapshot) []byte {
	snapSize := int(unsafe.Sizeof(*snap))
	buf := make([]byte, 2+snapSize)
	binary.BigEndian.PutUint16(buf[0:2], symbolIdx)
	snapSlice := unsafe.Slice((*byte)(unsafe.Pointer(snap)), snapSize)
	copy(buf[2:2+snapSize], snapSlice)
	return buf
}

// DecodeSnapshot decodes symbolIdx and SymbolSnapshot safely copying into an aligned struct.
func DecodeSnapshot(data []byte) (uint16, *shm.SymbolSnapshot, error) {
	snapSize := int(unsafe.Sizeof(shm.SymbolSnapshot{}))
	if len(data) < 2+snapSize {
		return 0, nil, io.ErrUnexpectedEOF
	}
	symbolIdx := binary.BigEndian.Uint16(data[0:2])
	var snap shm.SymbolSnapshot
	snapDst := unsafe.Slice((*byte)(unsafe.Pointer(&snap)), snapSize)
	copy(snapDst, data[2:2+snapSize])
	return symbolIdx, &snap, nil
}

// EncodeFrameSlot encodes phaseIdx, slotIdx, anchorNS, and raw slot bytes.
func EncodeFrameSlot(phaseIdx uint8, slotIdx uint32, anchorNS int64, rawSlotBytes []byte) []byte {
	headerLen := 1 + 4 + 8
	buf := make([]byte, headerLen+len(rawSlotBytes))
	buf[0] = phaseIdx
	binary.BigEndian.PutUint32(buf[1:5], slotIdx)
	binary.BigEndian.PutUint64(buf[5:13], uint64(anchorNS))
	copy(buf[13:], rawSlotBytes)
	return buf
}

// DecodeFrameSlot decodes phaseIdx, slotIdx, anchorNS, and raw slot bytes slice.
func DecodeFrameSlot(data []byte) (uint8, uint32, int64, []byte, error) {
	if len(data) < 13 {
		return 0, 0, 0, nil, io.ErrUnexpectedEOF
	}
	phaseIdx := data[0]
	slotIdx := binary.BigEndian.Uint32(data[1:5])
	anchorNS := int64(binary.BigEndian.Uint64(data[5:13]))
	rawBytes := data[13:]
	return phaseIdx, slotIdx, anchorNS, rawBytes, nil
}

// EncodeAnchorCommit encodes anchor timestamp and number of preceding snapshots.
func EncodeAnchorCommit(anchorNS int64, numSnapshots uint32) []byte {
	buf := make([]byte, 12)
	binary.BigEndian.PutUint64(buf[0:8], uint64(anchorNS))
	binary.BigEndian.PutUint32(buf[8:12], numSnapshots)
	return buf
}

// DecodeAnchorCommit decodes anchor timestamp and snapshot count.
func DecodeAnchorCommit(data []byte) (int64, uint32, error) {
	if len(data) < 12 {
		return 0, 0, io.ErrUnexpectedEOF
	}
	anchorNS := int64(binary.BigEndian.Uint64(data[0:8]))
	numSnapshots := binary.BigEndian.Uint32(data[8:12])
	return anchorNS, numSnapshots, nil
}

// WritePacket writes a length-prefixed packet: [4B len][1B msgType][8B seq][payload]
func WritePacket(w io.Writer, msgType uint8, seq uint64, payload []byte) error {
	totalLen := uint32(1 + 8 + len(payload))
	var header [13]byte
	binary.BigEndian.PutUint32(header[0:4], totalLen)
	header[4] = msgType
	binary.BigEndian.PutUint64(header[5:13], seq)

	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

// ReadPacket reads a length-prefixed packet from r.
func ReadPacket(r io.Reader) (uint8, uint64, []byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return 0, 0, nil, err
	}

	totalLen := binary.BigEndian.Uint32(lenBuf[:])
	if totalLen < 9 || totalLen > 16*1024*1024 {
		return 0, 0, nil, ErrCorruptMessage
	}

	payloadLen := totalLen - 9
	msgBuf := make([]byte, totalLen)
	if _, err := io.ReadFull(r, msgBuf); err != nil {
		return 0, 0, nil, err
	}

	msgType := msgBuf[0]
	seq := binary.BigEndian.Uint64(msgBuf[1:9])
	payload := msgBuf[9 : 9+payloadLen]

	return msgType, seq, payload, nil
}
