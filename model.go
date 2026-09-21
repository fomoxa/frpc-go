package frpc

import (
	"encoding/binary"
	"fmt"
	"unicode/utf8"
)

type ModelFormatError struct {
	Reason string
}

func (e *ModelFormatError) Error() string { return e.Reason }

type BodyReader struct {
	bytes  []byte
	cursor int
}

func NewBodyReader(bytes []byte) *BodyReader {
	return &BodyReader{bytes: bytes}
}

func (r *BodyReader) AtEnd() bool { return r.cursor >= len(r.bytes) }

func (r *BodyReader) ReadU32() (uint32, bool, error) {
	if r.AtEnd() {
		return 0, false, nil
	}
	if len(r.bytes)-r.cursor < 4 {
		return 0, false, &ModelFormatError{Reason: "the body ended inside a u32"}
	}
	value := binary.LittleEndian.Uint32(r.bytes[r.cursor:])
	r.cursor += 4
	return value, true, nil
}

func (r *BodyReader) ReadBytes() ([]byte, bool, error) {
	length, present, err := r.ReadU32()
	if err != nil || !present {
		return nil, present, err
	}
	if uint64(len(r.bytes)-r.cursor) < uint64(length) {
		return nil, false, &ModelFormatError{Reason: fmt.Sprintf("a length of %d runs past the end of the body", length)}
	}
	value := make([]byte, length)
	copy(value, r.bytes[r.cursor:])
	r.cursor += int(length)
	return value, true, nil
}

func (r *BodyReader) ReadString() (string, bool, error) {
	raw, present, err := r.ReadBytes()
	if err != nil || !present {
		return "", present, err
	}
	if !utf8.Valid(raw) {
		return "", false, &ModelFormatError{Reason: "a string is not valid UTF-8"}
	}
	return string(raw), true, nil
}

type BodyWriter struct {
	buf []byte
}

func NewBodyWriter() *BodyWriter {
	return &BodyWriter{buf: make([]byte, 0, 64)}
}

func (w *BodyWriter) PutU32(value uint32) *BodyWriter {
	w.buf = binary.LittleEndian.AppendUint32(w.buf, value)
	return w
}

func (w *BodyWriter) PutBytes(value []byte) *BodyWriter {
	w.PutU32(uint32(len(value)))
	w.buf = append(w.buf, value...)
	return w
}

func (w *BodyWriter) PutString(value string) *BodyWriter {
	w.PutU32(uint32(len(value)))
	w.buf = append(w.buf, value...)
	return w
}

func (w *BodyWriter) Len() int { return len(w.buf) }

func (w *BodyWriter) Bytes() []byte {
	out := make([]byte, len(w.buf))
	copy(out, w.buf)
	return out
}
