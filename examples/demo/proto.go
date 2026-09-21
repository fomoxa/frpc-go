package demo

import (
	"fmt"
	"strings"
	"time"

	frpc "github.com/fomoxa/frpc-go"
	"github.com/fomoxa/frpc-go/examples/demo/generated"
	"github.com/fomoxa/frpc-go/examples/demo/models"
)

const (
	VoidID            = generated.FRpcVoidRpcMessageID
	ErrorID           = generated.FRpcErrorRpcMessageID
	CreditID          = generated.FRpcCreditRpcMessageID
	CtxID             = generated.FRpcCtxRpcMessageID
	ReflectRequestID  = generated.FRpcReflectRequestRpcMessageID
	ReflectResponseID = generated.FRpcReflectResponseRpcMessageID

	EchoRequestID  = generated.EchoRequestRpcMessageID
	EchoResponseID = generated.EchoResponseRpcMessageID
	CountRequestID = generated.CountRequestRpcMessageID
	CountItemID    = generated.CountItemRpcMessageID
	UploadOpenID   = generated.UploadOpenRpcMessageID
	UploadChunkID  = generated.UploadChunkRpcMessageID
	UploadDoneID   = generated.UploadDoneRpcMessageID
	ChatOpenID     = generated.ChatOpenRpcMessageID
	ChatSaidID     = generated.ChatSaidRpcMessageID
	ChatHeardID    = generated.ChatHeardRpcMessageID
	TicksRequestID = generated.TicksRequestRpcMessageID
	TicksItemID    = generated.TicksItemRpcMessageID
)

var (
	Say     = frpc.Unary("Echo.Say", EchoRequestID, EchoResponseID)
	Count   = frpc.ServerStream("Echo.Count", CountRequestID, CountItemID)
	Upload  = frpc.ClientStream("Blob.Upload", UploadOpenID, UploadChunkID, UploadDoneID)
	Chat    = frpc.BidiStream("Room.Chat", ChatOpenID, ChatSaidID, ChatHeardID)
	Ticks   = frpc.ServerStream("Clock.Ticks", TicksRequestID, TicksItemID)
	Reflect = frpc.Unary(frpc.ReflectMethodName, ReflectRequestID, ReflectResponseID)
)

func Methods() []frpc.Method { return []frpc.Method{Say, Count, Upload, Chat, Ticks} }

func Builder() *frpc.RegistryBuilder { return frpc.NewRegistry(VoidID, ErrorID).Credit(CreditID) }

func CallerRegistry() *frpc.Registry {
	builder := Builder().Declare(Reflect)
	for _, method := range Methods() {
		builder.Declare(method)
	}
	return must(builder.Build())
}

func ResponderBuilder() *frpc.RegistryBuilder {
	return Builder().
		Serve(Say, func(_ frpc.Method, body []byte) (frpc.Handler, error) {
			text := DecodeEcho(body)
			if text == "" {
				return nil, frpc.InvalidArgument("Echo.Say needs a non-empty text")
			}
			return frpc.RespondNow(EncodeEchoResponse(strings.ToUpper(text))), nil
		}).
		Serve(Count, func(_ frpc.Method, body []byte) (frpc.Handler, error) {
			count := DecodeCount(body)
			if count > 100 {
				return nil, frpc.InvalidArgument("Echo.Count is capped at 100")
			}
			return &counter{count: count}, nil
		}).
		Serve(Upload, func(frpc.Method, []byte) (frpc.Handler, error) { return &uploadHandler{}, nil }).
		Serve(Chat, func(frpc.Method, []byte) (frpc.Handler, error) { return &chatHandler{}, nil }).
		Serve(Ticks, func(frpc.Method, []byte) (frpc.Handler, error) { return &ticksHandler{}, nil })
}

func ResponderRegistry(reflection bool) *frpc.Registry {
	builder := ResponderBuilder()
	if reflection {
		builder.Reflection(ReflectRequestID, ReflectResponseID, SchemaJSON)
	}
	return must(builder.Build())
}

func must(registry *frpc.Registry, err error) *frpc.Registry {
	if err != nil {
		panic(err)
	}
	return registry
}

func encode(write func(w *generated.Writer)) []byte {
	writer := generated.NewWriter()
	write(writer)
	return writer.Bytes()
}

func decode(body []byte, read func(r *generated.Reader) error) {
	if err := read(generated.NewReader(body)); err != nil {
		panic(frpc.InvalidArgument(err.Error()))
	}
}

func EncodeEcho(text string) []byte {
	return encode(func(w *generated.Writer) {
		generated.EchoRequestRpcCodec{}.Encode(w, &models.EchoRequest{Text: text})
	})
}

func DecodeEcho(body []byte) string {
	var value models.EchoRequest
	decode(body, func(r *generated.Reader) error { return generated.EchoRequestRpcCodec{}.Decode(r, &value) })
	return value.Text
}

func EncodeEchoResponse(text string) []byte {
	return encode(func(w *generated.Writer) {
		generated.EchoResponseRpcCodec{}.Encode(w, &models.EchoResponse{Text: text})
	})
}

func DecodeEchoResponse(body []byte) string {
	var value models.EchoResponse
	decode(body, func(r *generated.Reader) error { return generated.EchoResponseRpcCodec{}.Decode(r, &value) })
	return value.Text
}

func EncodeCount(count uint32) []byte {
	return encode(func(w *generated.Writer) {
		generated.CountRequestRpcCodec{}.Encode(w, &models.CountRequest{Count: count})
	})
}

func DecodeCount(body []byte) uint32 {
	var value models.CountRequest
	decode(body, func(r *generated.Reader) error { return generated.CountRequestRpcCodec{}.Decode(r, &value) })
	return value.Count
}

func EncodeCountItem(item uint32) []byte {
	return encode(func(w *generated.Writer) {
		generated.CountItemRpcCodec{}.Encode(w, &models.CountItem{Value: item})
	})
}

func DecodeCountItem(body []byte) uint32 {
	var value models.CountItem
	decode(body, func(r *generated.Reader) error { return generated.CountItemRpcCodec{}.Decode(r, &value) })
	return value.Value
}

func EncodeUploadOpen(name string) []byte {
	return encode(func(w *generated.Writer) {
		generated.UploadOpenRpcCodec{}.Encode(w, &models.UploadOpen{Name: name})
	})
}

func EncodeChunk(size uint32) []byte {
	return encode(func(w *generated.Writer) {
		generated.UploadChunkRpcCodec{}.Encode(w, &models.UploadChunk{Size: size})
	})
}

func DecodeChunk(body []byte) uint32 {
	var value models.UploadChunk
	decode(body, func(r *generated.Reader) error { return generated.UploadChunkRpcCodec{}.Decode(r, &value) })
	return value.Size
}

func EncodeDone(summary string) []byte {
	return encode(func(w *generated.Writer) {
		generated.UploadDoneRpcCodec{}.Encode(w, &models.UploadDone{Summary: summary})
	})
}

func DecodeDone(body []byte) string {
	var value models.UploadDone
	decode(body, func(r *generated.Reader) error { return generated.UploadDoneRpcCodec{}.Decode(r, &value) })
	return value.Summary
}

func EncodeRoom(room string) []byte {
	return encode(func(w *generated.Writer) {
		generated.ChatOpenRpcCodec{}.Encode(w, &models.ChatOpen{Room: room})
	})
}

func EncodeSaid(line string) []byte {
	return encode(func(w *generated.Writer) {
		generated.ChatSaidRpcCodec{}.Encode(w, &models.ChatSaid{Line: line})
	})
}

func DecodeSaid(body []byte) string {
	var value models.ChatSaid
	decode(body, func(r *generated.Reader) error { return generated.ChatSaidRpcCodec{}.Decode(r, &value) })
	return value.Line
}

func EncodeHeard(line string) []byte {
	return encode(func(w *generated.Writer) {
		generated.ChatHeardRpcCodec{}.Encode(w, &models.ChatHeard{Line: line})
	})
}

func DecodeHeard(body []byte) string {
	var value models.ChatHeard
	decode(body, func(r *generated.Reader) error { return generated.ChatHeardRpcCodec{}.Decode(r, &value) })
	return value.Line
}

func EncodeTicksRequest() []byte {
	return encode(func(w *generated.Writer) {
		generated.TicksRequestRpcCodec{}.Encode(w, &models.TicksRequest{})
	})
}

func EncodeTick(tick uint32) []byte {
	return encode(func(w *generated.Writer) {
		generated.TicksItemRpcCodec{}.Encode(w, &models.TicksItem{Value: tick})
	})
}

func DecodeTick(body []byte) uint32 {
	var value models.TicksItem
	decode(body, func(r *generated.Reader) error { return generated.TicksItemRpcCodec{}.Decode(r, &value) })
	return value.Value
}

func EncodeReflectRequest() []byte {
	return encode(func(w *generated.Writer) {
		generated.FRpcReflectRequestRpcCodec{}.Encode(w, &models.FRpcReflectRequest{})
	})
}

func DecodeReflectResponse(body []byte) models.FRpcReflectResponse {
	var value models.FRpcReflectResponse
	decode(body, func(r *generated.Reader) error {
		return generated.FRpcReflectResponseRpcCodec{}.Decode(r, &value)
	})
	return value
}

type counter struct {
	count uint32
	next  uint32
}

func (h *counter) Poll(time.Time) frpc.Progress {
	if h.next >= h.count {
		return frpc.End()
	}
	h.next++
	return frpc.Item(EncodeCountItem(h.next))
}

type uploadHandler struct {
	bytes  uint32
	chunks uint32
	closed bool
}

func (h *uploadHandler) Poll(time.Time) frpc.Progress {
	if h.closed {
		return frpc.Respond(EncodeDone(fmt.Sprintf("%d chunks, %d bytes", h.chunks, h.bytes)))
	}
	return frpc.Pending()
}

func (h *uploadHandler) Item(body []byte) error {
	size := DecodeChunk(body)
	if size == 0 {
		return frpc.InvalidArgument("an empty chunk is not accepted")
	}
	h.chunks++
	h.bytes += size
	return nil
}

func (h *uploadHandler) EndOfStream() error {
	h.closed = true
	return nil
}

type chatHandler struct {
	replies [][]byte
	closed  bool
}

func (h *chatHandler) Poll(time.Time) frpc.Progress {
	if len(h.replies) > 0 {
		reply := h.replies[0]
		h.replies = h.replies[1:]
		return frpc.Item(reply)
	}
	if h.closed {
		return frpc.End()
	}
	return frpc.Pending()
}

func (h *chatHandler) Item(body []byte) error {
	h.replies = append(h.replies, EncodeHeard(strings.ToUpper(DecodeSaid(body))))
	return nil
}

func (h *chatHandler) EndOfStream() error {
	h.closed = true
	return nil
}

type ticksHandler struct {
	next uint32
}

func (h *ticksHandler) Poll(time.Time) frpc.Progress {
	h.next++
	return frpc.Item(EncodeTick(h.next))
}
