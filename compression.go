package frpc

import (
	"bytes"
	"compress/flate"
	"errors"
	"io"
)

var (
	ErrMalformedBody = errors.New("frpc: the compressed body is malformed")
	ErrExceedsLimit  = errors.New("frpc: the body expands past the ceiling")
)

type Compressor interface {
	Compress(body []byte) ([]byte, bool)
	Decompress(body []byte, limit int) ([]byte, error)
}

type DeflateCompressor struct{}

const inflateChunk = 16 * 1024

func (DeflateCompressor) Compress(body []byte) ([]byte, bool) {
	var out bytes.Buffer
	writer, err := flate.NewWriter(&out, flate.BestSpeed)
	if err != nil {
		return nil, false
	}
	if _, err := writer.Write(body); err != nil {
		return nil, false
	}
	if err := writer.Close(); err != nil {
		return nil, false
	}
	return out.Bytes(), true
}

func (DeflateCompressor) Decompress(body []byte, limit int) ([]byte, error) {
	reader := flate.NewReader(bytes.NewReader(body))
	defer reader.Close()
	var out bytes.Buffer
	chunk := make([]byte, inflateChunk)
	for {
		read, err := reader.Read(chunk)
		if out.Len()+read > limit {
			return nil, ErrExceedsLimit
		}
		out.Write(chunk[:read])
		if err == io.EOF {
			return out.Bytes(), nil
		}
		if err != nil {
			return nil, ErrMalformedBody
		}
	}
}
