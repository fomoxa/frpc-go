package frpc_test

import (
	"bytes"
	"slices"
	"testing"
	"time"

	frpc "github.com/fomoxa/frpc-go"
	"github.com/fomoxa/frpc-go/examples/demo"
)

type sent struct {
	from      frpc.Sender
	messageID uint32
	payload   []byte
}

func (s sent) kind() frpc.Kind { return frpc.KindBits(s.payload[0]) }

func (s sent) call() frpc.CallID { return frpc.ReadCallID(s.payload) }

func (s sent) code() uint32 { return frpc.DecodeStatus(s.payload[frpc.HeaderLen:]).Code }

type link struct {
	t        *testing.T
	caller   *frpc.Core
	callee   *frpc.Core
	now      time.Time
	wire     []sent
	outcomes []frpc.Outcome
}

type linkSetup struct {
	caller       *frpc.Registry
	callee       *frpc.Registry
	callerConfig frpc.Config
	calleeConfig frpc.Config
}

func newLink(t *testing.T, setup linkSetup) *link {
	t.Helper()
	if setup.caller == nil {
		setup.caller = demo.DriverRegistry()
	}
	if setup.callee == nil {
		setup.callee = demo.ResponderRegistry(true)
	}
	l := &link{
		t:      t,
		caller: frpc.NewCore(setup.caller, frpc.RoleClient, setup.callerConfig),
		callee: frpc.NewCore(setup.callee, frpc.RoleServer, setup.calleeConfig),
		now:    time.Unix(100, 0),
	}
	l.caller.OnReady()
	l.callee.OnReady()
	return l
}

func (l *link) open(requestID uint32, body []byte, ctx *frpc.Ctx, options frpc.CallOptions) frpc.CallID {
	l.t.Helper()
	call, err := l.caller.Call(requestID, body, ctx, options, l.now)
	if err != nil {
		l.t.Fatalf("the call opens: %v", err)
	}
	return call
}

func (l *link) openPlain(requestID uint32, body []byte) frpc.CallID {
	l.t.Helper()
	return l.open(requestID, body, nil, frpc.WithoutDeadline())
}

func (l *link) pump(rounds int) {
	for range rounds {
		for _, frame := range l.drainCaller() {
			l.callee.OnMessage(frame.messageID, frame.payload, l.now)
		}
		l.callee.Tick(l.now)
		for _, frame := range l.drainCallee() {
			l.caller.OnMessage(frame.messageID, frame.payload, l.now)
		}
		l.caller.Tick(l.now)
		l.outcomes = append(l.outcomes, l.caller.TakeOutcomes()...)
	}
}

func (l *link) drainCaller() []sent { return l.drain(l.caller, frpc.SenderCaller) }

func (l *link) drainCallee() []sent { return l.drain(l.callee, frpc.SenderCallee) }

func (l *link) toCallee(messageID uint32, payload []byte) bool {
	return l.callee.OnMessage(messageID, payload, l.now)
}

func (l *link) toCaller(messageID uint32, payload []byte) {
	l.caller.OnMessage(messageID, payload, l.now)
	l.outcomes = append(l.outcomes, l.caller.TakeOutcomes()...)
}

func (l *link) forCall(call frpc.CallID) []frpc.Outcome {
	var seen []frpc.Outcome
	for _, outcome := range l.outcomes {
		if outcome.Call == call {
			seen = append(seen, outcome)
		}
	}
	return seen
}

func (l *link) last(call frpc.CallID) frpc.Outcome {
	l.t.Helper()
	seen := l.forCall(call)
	if len(seen) == 0 {
		l.t.Fatalf("%s has no outcome", call)
	}
	return seen[len(seen)-1]
}

func (l *link) sentBy(side frpc.Sender) []sent {
	var frames []sent
	for _, frame := range l.wire {
		if frame.from == side {
			frames = append(frames, frame)
		}
	}
	return frames
}

func (l *link) drain(core *frpc.Core, side frpc.Sender) []sent {
	var frames []sent
	core.Drain(func(frame frpc.Outgoing) frpc.Delivery {
		frames = append(frames, sent{from: side, messageID: frame.MessageID, payload: frame.Payload})
		return frpc.Accepted
	})
	l.wire = append(l.wire, frames...)
	return frames
}

func frame(kind frpc.Kind, call frpc.CallID, body []byte, extraBits byte) []byte {
	payload := frpc.Encode(frpc.Header{Kind: kind, Call: call}, nil, body)
	payload[0] |= extraBits
	return payload
}

func credit(items uint32) []byte { return frpc.NewBodyWriter().PutU32(items).Bytes() }

var slow = frpc.Unary("Test.Slow", demo.EchoRequestID, demo.EchoResponseID)

func build(t *testing.T, builder *frpc.RegistryBuilder) *frpc.Registry {
	t.Helper()
	registry, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func slowCaller(t *testing.T) *frpc.Registry {
	return build(t, demo.Builder().Declare(slow).Declare(demo.Upload))
}

func slowCallee(t *testing.T, handler func() frpc.Handler) *frpc.Registry {
	if handler == nil {
		handler = func() frpc.Handler { return &forever{} }
	}
	return build(t, demo.Builder().
		Serve(slow, func(frpc.Method, []byte) (frpc.Handler, error) { return handler(), nil }).
		Serve(demo.Upload, func(frpc.Method, []byte) (frpc.Handler, error) { return &forever{}, nil }))
}

func serving(handler frpc.Handler) frpc.HandlerFactory {
	return func(frpc.Method, []byte) (frpc.Handler, error) { return handler, nil }
}

type forever struct {
	cancelled bool
	items     [][]byte
}

func (h *forever) Poll(time.Time) frpc.Progress { return frpc.Pending() }

func (h *forever) Cancel() { h.cancelled = true }

func (h *forever) Item(body []byte) error {
	h.items = append(h.items, body)
	return nil
}

func expectFailed(t *testing.T, outcome frpc.Outcome, code uint32, what string) *frpc.Status {
	t.Helper()
	if outcome.Kind != frpc.OutcomeFailed || outcome.Status.Code != code {
		t.Fatalf("%s: expected %s, got %s", what, frpc.CodeName(code), outcome)
	}
	return outcome.Status
}

func expectEqual[T comparable](t *testing.T, expected, actual T, what string) {
	t.Helper()
	if expected != actual {
		t.Fatalf("%s: expected %v, got %v", what, expected, actual)
	}
}

func expectSequence[T comparable](t *testing.T, expected, actual []T, what string) {
	t.Helper()
	if !slices.Equal(expected, actual) {
		t.Fatalf("%s: expected %v, got %v", what, expected, actual)
	}
}

func expectBytes(t *testing.T, expected, actual []byte, what string) {
	t.Helper()
	if !bytes.Equal(expected, actual) {
		t.Fatalf("%s: expected [%X], got [%X]", what, expected, actual)
	}
}

func expectTrue(t *testing.T, condition bool, what string) {
	t.Helper()
	if !condition {
		t.Fatal(what)
	}
}

func only(t *testing.T, frames []sent) sent {
	t.Helper()
	if len(frames) != 1 {
		t.Fatalf("expected exactly one frame, got %d", len(frames))
	}
	return frames[0]
}

func kinds(frames []sent) []frpc.Kind {
	var out []frpc.Kind
	for _, frame := range frames {
		out = append(out, frame.kind())
	}
	return out
}
