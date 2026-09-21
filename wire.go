package frpc

import (
	"encoding/binary"
	"fmt"
)

type Kind uint8

const (
	KindRequest Kind = iota
	KindResponse
	KindError
	KindItem
	KindEnd
	KindCancel
	KindCredit
)

func (k Kind) String() string {
	switch k {
	case KindRequest:
		return "REQUEST"
	case KindResponse:
		return "RESPONSE"
	case KindError:
		return "ERROR"
	case KindItem:
		return "ITEM"
	case KindEnd:
		return "END"
	case KindCancel:
		return "CANCEL"
	case KindCredit:
		return "CREDIT"
	default:
		return fmt.Sprintf("kind %d", uint8(k))
	}
}

type Sender int

const (
	SenderCaller Sender = iota
	SenderCallee
)

type CallID uint32

func (c CallID) Even() bool { return c%2 == 0 }

func (c CallID) String() string { return fmt.Sprintf("call %d", uint32(c)) }

type Header struct {
	Kind       Kind
	Call       CallID
	Compressed bool
	HasCtx     bool
}

type WireError int

const (
	WireOK WireError = iota
	WireTruncated
	WireUnknownKind
	WireReservedBits
	WireCtxOnNonRequest
	WireCtxTruncated
	WireCtxTooLarge
)

type Parsed struct {
	Header Header
	Ctx    []byte
	Body   []byte
}

const (
	HeaderLen    = 5
	CtxLengthLen = 4

	flagCompressed byte = 0b1000_0000
	flagCtx        byte = 0b0100_0000
	reservedMask   byte = 0b0011_1000
	kindMask       byte = 0b0000_0111
)

func KindBits(first byte) Kind { return Kind(first & kindMask) }

func Encode(header Header, ctx []byte, body []byte) []byte {
	size := HeaderLen + len(body)
	if header.HasCtx {
		size += CtxLengthLen + len(ctx)
	}
	payload := make([]byte, HeaderLen, size)
	first := byte(header.Kind)
	if header.Compressed {
		first |= flagCompressed
	}
	if header.HasCtx {
		first |= flagCtx
	}
	payload[0] = first
	binary.LittleEndian.PutUint32(payload[1:HeaderLen], uint32(header.Call))
	if header.HasCtx {
		payload = binary.LittleEndian.AppendUint32(payload, uint32(len(ctx)))
		payload = append(payload, ctx...)
	}
	return append(payload, body...)
}

func ReadCallID(payload []byte) CallID {
	return CallID(binary.LittleEndian.Uint32(payload[1:HeaderLen]))
}

func Decode(payload []byte, maxCtxLen int) (Parsed, WireError) {
	if len(payload) < HeaderLen {
		return Parsed{}, WireTruncated
	}
	first := payload[0]
	if first&reservedMask != 0 {
		return Parsed{}, WireReservedBits
	}
	kind := KindBits(first)
	if kind > KindCredit {
		return Parsed{}, WireUnknownKind
	}
	header := Header{
		Kind:       kind,
		Call:       ReadCallID(payload),
		Compressed: first&flagCompressed != 0,
		HasCtx:     first&flagCtx != 0,
	}
	rest := payload[HeaderLen:]
	if !header.HasCtx {
		return Parsed{Header: header, Body: rest}, WireOK
	}
	if header.Kind != KindRequest {
		return Parsed{}, WireCtxOnNonRequest
	}
	if len(rest) < CtxLengthLen {
		return Parsed{}, WireCtxTruncated
	}
	declared := uint64(binary.LittleEndian.Uint32(rest))
	if declared > uint64(maxCtxLen) {
		return Parsed{}, WireCtxTooLarge
	}
	afterLength := rest[CtxLengthLen:]
	if uint64(len(afterLength)) < declared {
		return Parsed{}, WireCtxTruncated
	}
	return Parsed{Header: header, Ctx: afterLength[:declared], Body: afterLength[declared:]}, WireOK
}

func Describe(err WireError, payload []byte) string {
	var first byte
	if len(payload) > 0 {
		first = payload[0]
	}
	switch err {
	case WireTruncated:
		return "payload ended before the 5-byte header"
	case WireUnknownKind:
		return fmt.Sprintf("kind %d is not 0..=6", uint8(KindBits(first)))
	case WireReservedBits:
		return fmt.Sprintf("reserved bits set in kind byte 0x%02X", first)
	case WireCtxOnNonRequest:
		return fmt.Sprintf("ctx block present on %s", KindBits(first))
	case WireCtxTruncated:
		return "payload ended inside the ctx block"
	case WireCtxTooLarge:
		return "ctx block exceeds the ceiling"
	default:
		return "no error"
	}
}
