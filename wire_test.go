package frpc_test

import (
	"encoding/binary"
	"encoding/hex"
	"testing"
	"time"

	frpc "github.com/fomoxa/frpc-go"
	"github.com/fomoxa/frpc-go/examples/demo"
)

func TestTheByteExampleEncodesByteForByte(t *testing.T) {
	body := frpc.NewBodyWriter().PutU32(7).Bytes()
	request := frpc.Encode(frpc.Header{Kind: frpc.KindRequest, Call: 4}, nil, body)
	expected, _ := hex.DecodeString("000400000007000000")
	expectBytes(t, expected, request, "REQUEST payload")
	end := frpc.Encode(frpc.Header{Kind: frpc.KindEnd, Call: 4}, nil, nil)
	expected, _ = hex.DecodeString("0404000000")
	expectBytes(t, expected, end, "END payload")

	parsed, err := frpc.Decode(request, 64*1024)
	expectEqual(t, frpc.WireOK, err, "decodes")
	expectEqual(t, frpc.KindRequest, parsed.Header.Kind, "kind")
	expectEqual(t, frpc.CallID(4), parsed.Header.Call, "call id")
	expectBytes(t, body, parsed.Body, "body")
}

func TestTheHeaderIsExactlyFiveBytesForEndAndCancel(t *testing.T) {
	for _, kind := range []frpc.Kind{frpc.KindEnd, frpc.KindCancel} {
		payload := frpc.Encode(frpc.Header{Kind: kind, Call: 0xABCDEF01}, nil, nil)
		expectEqual(t, frpc.HeaderLen, len(payload), kind.String()+" length")
		expectBytes(t, []byte{byte(kind), 0x01, 0xEF, 0xCD, 0xAB}, payload, kind.String()+" bytes")
	}
}

func TestKindSevenAnswersInternalAndTheSessionStaysAlive(t *testing.T) {
	rejectedAsInternal(t, 0x07)
}

func TestAReservedBitAnswersInternalAndTheSessionStaysAlive(t *testing.T) {
	rejectedAsInternal(t, 0b0000_1000)
}

func rejectedAsInternal(t *testing.T, extraBits byte) {
	l := newLink(t, linkSetup{})
	l.toCallee(demo.EchoRequestID, frame(frpc.KindRequest, 2, demo.EncodeEcho("x"), extraBits))
	answer := only(t, l.drainCallee())
	expectEqual(t, frpc.KindError, answer.kind(), "the reply kind")
	expectEqual(t, frpc.CallID(2), answer.call(), "the reply names the call")
	expectEqual(t, frpc.CodeInternal, answer.code(), "the code")

	call := l.openPlain(demo.EchoRequestID, demo.EncodeEcho("still here"))
	l.pump(4)
	expectEqual(t, frpc.OutcomeResponse, l.last(call).Kind, "a later call is served")
}

func TestBitSixClearPutsTheRequestModelAtByteFive(t *testing.T) {
	l := newLink(t, linkSetup{})
	l.openPlain(demo.EchoRequestID, demo.EncodeEcho("plain"))
	request := only(t, l.drainCaller())
	expectEqual(t, byte(0), request.payload[0]&0x40, "bit 6 is clear")
	expectBytes(t, demo.EncodeEcho("plain"), request.payload[5:], "the model starts at byte 5")
}

func TestALongerCtxFromANewerSenderLeavesTheRequestModelIntact(t *testing.T) {
	l := newLink(t, linkSetup{})
	ctx := frpc.NewBodyWriter().PutU32(0).PutBytes([]byte{9}).PutBytes(nil).PutString("").PutString("acme").
		PutBytes(nil).PutU32(0).PutU32(0xFEEDFACE).PutString("an eighth field").Bytes()
	payload := frpc.Encode(frpc.Header{Kind: frpc.KindRequest, Call: 0, HasCtx: true}, ctx, demo.EncodeEcho("skewed"))
	l.toCallee(demo.EchoRequestID, payload)
	answer := only(t, l.drainCallee())
	expectEqual(t, frpc.KindResponse, answer.kind(), "served")
	expectEqual(t, "SKEWED", demo.DecodeEchoResponse(answer.payload[5:]), "the model decoded intact")
	decoded, err := frpc.DecodeCtx(ctx)
	expectTrue(t, err == nil, "the ctx decodes")
	expectEqual(t, "acme", decoded.Tenant, "the known fields decode")
}

func TestACtxLengthAbove64KiBIsRefusedBeforeAllocation(t *testing.T) {
	payload := make([]byte, 5+4)
	payload[0] = 0x40
	binary.LittleEndian.PutUint32(payload[5:], 64*1024+1)
	_, err := frpc.Decode(payload, 64*1024)
	expectEqual(t, frpc.WireCtxTooLarge, err, "refused from the length alone")

	l := newLink(t, linkSetup{})
	l.toCallee(demo.EchoRequestID, payload)
	expectEqual(t, frpc.CodeInternal, only(t, l.drainCallee()).code(), "answered INTERNAL")
}

func TestBitSixOnAKindOtherThanRequestIsInternal(t *testing.T) {
	l := newLink(t, linkSetup{})
	call := l.openPlain(demo.CountRequestID, demo.EncodeCount(1))
	l.drainCaller()
	l.toCaller(demo.CountItemID, frame(frpc.KindItem, call, demo.EncodeCountItem(1), 0x40))
	expectFailed(t, l.last(call), frpc.CodeInternal, "ctx on an ITEM")
}

func TestZeroDeadlineMsSetsNoDeadline(t *testing.T) {
	var seen time.Time
	callee := build(t, demo.Builder().Serve(slow, serving(&forever{})))
	l := newLink(t, linkSetup{caller: slowCaller(t), callee: callee})
	l.callee.AddInterceptor(&probe{seen: func(context *frpc.CallContext) { seen = context.Deadline }})
	l.open(slow.RequestID, demo.EncodeEcho("x"), frpc.WithDeadlineMs(0), frpc.WithoutDeadline())
	l.pump(1)
	expectEqual(t, l.now.Add(30*time.Second), seen, "only the callee's own unary limit applies")

	for _, frame := range l.wire {
		if frame.kind() == frpc.KindRequest {
			expectEqual(t, byte(0), frame.payload[0]&0x40, "a ctx of unset fields is not sent at all")
		}
	}
}

func TestDeadlineMsShrinksAcrossThreeHops(t *testing.T) {
	var heardAtC uint32
	hopC := build(t, demo.Builder().Serve(slow, serving(&forever{})))

	var onward *frpc.Ctx
	hopB := build(t, demo.Builder().Serve(slow, serving(&forever{})))
	linkAB := newLink(t, linkSetup{caller: slowCaller(t), callee: hopB})
	linkAB.callee.AddInterceptor(&probe{seen: func(context *frpc.CallContext) {
		onward, _ = context.OnwardCtx(context.Now.Add(300 * time.Millisecond))
	}})
	linkAB.open(slow.RequestID, demo.EncodeEcho("x"),
		&frpc.Ctx{DeadlineMs: 2000, TraceID: []byte{7}, Token: "secret"}, frpc.WithoutDeadline())
	linkAB.pump(1)
	expectTrue(t, onward != nil, "B computed an onward ctx")
	expectEqual(t, uint32(1700), onward.DeadlineMs, "B passes the remaining budget")
	expectBytes(t, []byte{7}, onward.TraceID, "the trace id is copied")
	expectEqual(t, "", onward.Token, "the token is not copied")

	linkBC := newLink(t, linkSetup{caller: slowCaller(t), callee: hopC})
	linkBC.callee.AddInterceptor(&probe{seen: func(context *frpc.CallContext) { heardAtC = context.Ctx.DeadlineMs }})
	linkBC.open(slow.RequestID, demo.EncodeEcho("x"), onward, frpc.WithoutDeadline())
	linkBC.pump(1)
	expectTrue(t, heardAtC > 0 && heardAtC < 2000, "C receives less than A sent")
}

func TestAnExhaustedBudgetMakesNoOnwardCall(t *testing.T) {
	var onward *frpc.Ctx
	var allowed bool
	hopB := build(t, demo.Builder().Serve(slow, serving(&forever{})))
	l := newLink(t, linkSetup{caller: slowCaller(t), callee: hopB})
	l.callee.AddInterceptor(&probe{seen: func(context *frpc.CallContext) {
		onward, allowed = context.OnwardCtx(context.Now.Add(2 * time.Second))
	}})
	l.open(slow.RequestID, demo.EncodeEcho("x"), frpc.WithDeadlineMs(2000), frpc.WithoutDeadline())
	l.pump(1)
	expectTrue(t, !allowed && onward == nil, "a spent budget is never sent on as deadline_ms = 0")
}

func TestTheShorterOfCallerAndCtxDeadlineTravels(t *testing.T) {
	l := newLink(t, linkSetup{})
	l.open(demo.EchoRequestID, demo.EncodeEcho("x"), frpc.WithDeadlineMs(9000), frpc.Within(1200*time.Millisecond))
	request := only(t, l.drainCaller())
	parsed, err := frpc.Decode(request.payload, 65536)
	expectEqual(t, frpc.WireOK, err, "decodes")
	ctx, _ := frpc.DecodeCtx(parsed.Ctx)
	expectEqual(t, uint32(1200), ctx.DeadlineMs, "the shorter deadline")
}

type probe struct {
	frpc.PassThrough
	seen func(*frpc.CallContext)
}

func (p *probe) Inbound(context *frpc.CallContext) *frpc.Status {
	p.seen(context)
	return nil
}
