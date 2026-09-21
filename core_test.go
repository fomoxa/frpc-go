package frpc_test

import (
	"errors"
	"testing"
	"time"

	frpc "github.com/fomoxa/frpc-go"
	"github.com/fomoxa/frpc-go/examples/demo"
)

func TestCallIDParityFollowsTheRole(t *testing.T) {
	l := newLink(t, linkSetup{})
	first := l.openPlain(demo.EchoRequestID, demo.EncodeEcho("a"))
	second := l.openPlain(demo.EchoRequestID, demo.EncodeEcho("b"))
	expectSequence(t, []frpc.CallID{0, 2}, []frpc.CallID{first, second}, "client ids")

	server := frpc.NewCore(demo.DriverRegistry(), frpc.RoleServer, frpc.Config{})
	server.OnReady()
	opened, err := server.Call(demo.EchoRequestID, demo.EncodeEcho("c"), nil, frpc.DefaultCallOptions(), time.Unix(1, 0))
	expectTrue(t, err == nil, "the server opens a call")
	expectEqual(t, frpc.CallID(1), opened, "server ids are odd")
}

func TestTheAllocatorNeverHandsOutAPendingID(t *testing.T) {
	allocator := frpc.NewCallIDAllocator(frpc.RoleClient)
	allocated, ok := allocator.Allocate(func(id frpc.CallID) bool { return id == 0 || id == 2 })
	expectTrue(t, ok, "an id is free")
	expectEqual(t, frpc.CallID(4), allocated, "skips 0 and 2")
}

func TestAResponseForACallNoLongerPendingIsIgnored(t *testing.T) {
	l := newLink(t, linkSetup{})
	l.toCaller(demo.EchoResponseID, frame(frpc.KindResponse, 40, demo.EncodeEchoResponse("late"), 0))
	expectEqual(t, 0, len(l.outcomes), "no outcome")
	expectEqual(t, 0, len(l.drainCaller()), "no frame")
}

func TestARequestWithNoHandlerIsUnimplemented(t *testing.T) {
	l := newLink(t, linkSetup{callee: build(t, demo.Builder().Declare(demo.Say))})
	call := l.openPlain(demo.EchoRequestID, demo.EncodeEcho("x"))
	l.pump(4)
	expectFailed(t, l.last(call), frpc.CodeUnimplemented, "declared but not served")
}

func TestARequestInNeitherSetIsUnimplementedAndNeverReachesTheApplication(t *testing.T) {
	l := newLink(t, linkSetup{})
	call := l.openPlain(demo.Missing.RequestID, nil)
	request := only(t, l.drainCaller())
	expectTrue(t, l.callee.OnMessage(request.messageID, request.payload, l.now), "fRPC keeps the frame")
	l.callee.Tick(l.now)
	for _, frame := range l.drainCallee() {
		l.toCaller(frame.messageID, frame.payload)
	}
	expectFailed(t, l.last(call), frpc.CodeUnimplemented, "unknown request type")
}

func TestAFrameInNeitherSetWithABrokenHeaderIsDroppedSilently(t *testing.T) {
	l := newLink(t, linkSetup{})
	expectTrue(t, l.toCallee(0x12345678, []byte{0x07, 2, 0, 0, 0}), "not for the application")
	expectTrue(t, l.toCallee(0x12345678, []byte{0x01}), "a short frame is not either")
	expectEqual(t, 0, len(l.drainCallee()), "no reply to an untrusted call id")
}

func TestADeclaredPlainMessageGoesToTheApplicationUnread(t *testing.T) {
	registry := build(t, demo.Builder().PlainMessage(0x0BADF00D).Serve(demo.Say,
		func(_ frpc.Method, body []byte) (frpc.Handler, error) { return frpc.RespondNow(body), nil }))
	core := frpc.NewCore(registry, frpc.RoleServer, frpc.Config{})
	core.OnReady()
	expectTrue(t, !core.OnMessage(0x0BADF00D, []byte{0x07, 0xFF}, time.Unix(1, 0)), "handed to the application")
	core.Drain(func(frpc.Outgoing) frpc.Delivery {
		t.Fatal("nothing is answered")
		return frpc.Accepted
	})
}

func TestTheCalleePendingCeilingAnswersResourceExhaustedAndSparesOtherCalls(t *testing.T) {
	l := newLink(t, linkSetup{caller: slowCaller(t), callee: slowCallee(t, nil), calleeConfig: frpc.Config{MaxPending: 1}})
	held := l.openPlain(slow.RequestID, demo.EncodeEcho("a"))
	refused := l.openPlain(slow.RequestID, demo.EncodeEcho("b"))
	l.pump(4)
	expectFailed(t, l.last(refused), frpc.CodeResourceExhausted, "the second call")
	expectEqual(t, 0, len(l.forCall(held)), "the first call is untouched")
	expectEqual(t, 1, l.callee.InflightCount(), "one call still running")
}

func TestTheCallerPendingCeilingIsALocalError(t *testing.T) {
	l := newLink(t, linkSetup{callerConfig: frpc.Config{MaxPending: 1}})
	l.openPlain(demo.EchoRequestID, demo.EncodeEcho("a"))
	_, err := l.caller.Call(demo.EchoRequestID, demo.EncodeEcho("b"), nil, frpc.DefaultCallOptions(), l.now)
	expectTrue(t, errors.Is(err, frpc.ErrTooManyPending), "local error")
	expectEqual(t, 1, len(l.drainCaller()), "only the first REQUEST is on the wire")
}

func TestARequestReusingARunningCallIDIsIgnored(t *testing.T) {
	l := newLink(t, linkSetup{caller: slowCaller(t), callee: slowCallee(t, nil)})
	call := l.openPlain(slow.RequestID, demo.EncodeEcho("a"))
	l.pump(4)
	l.toCallee(slow.RequestID, frame(frpc.KindRequest, call, demo.EncodeEcho("again"), 0))
	l.callee.Tick(l.now)
	expectEqual(t, 0, len(l.drainCallee()), "nothing answers the duplicate")
	expectEqual(t, 1, l.callee.InflightCount(), "the original call continues")
}

func TestWrongCallIDParityIsInvalidArgument(t *testing.T) {
	l := newLink(t, linkSetup{})
	l.toCallee(demo.EchoRequestID, frame(frpc.KindRequest, 3, demo.EncodeEcho("odd"), 0))
	expectEqual(t, frpc.CodeInvalidArgument, only(t, l.drainCallee()).code(), "code")
}

func TestAnUndecodableErrorCompletesTheCallWithInternal(t *testing.T) {
	l := newLink(t, linkSetup{})
	call := l.openPlain(demo.EchoRequestID, demo.EncodeEcho("x"))
	l.drainCaller()
	l.toCaller(demo.ErrorID, frame(frpc.KindError, call, []byte{1, 2}, 0))
	expectFailed(t, l.last(call), frpc.CodeInternal, "undecodable error body")
}

func TestAResponseWithTheWrongMessageIDIsInvalidArgument(t *testing.T) {
	l := newLink(t, linkSetup{})
	call := l.openPlain(demo.EchoRequestID, demo.EncodeEcho("x"))
	l.drainCaller()
	l.toCaller(demo.CountItemID, frame(frpc.KindResponse, call, demo.EncodeCountItem(1), 0))
	expectFailed(t, l.last(call), frpc.CodeInvalidArgument, "wrong response type")
}

func TestEndWithAnIDOtherThanVoidIsInvalidArgumentBothDirections(t *testing.T) {
	l := newLink(t, linkSetup{})
	count := l.openPlain(demo.CountRequestID, demo.EncodeCount(0))
	l.drainCaller()
	l.toCaller(demo.ErrorID, frame(frpc.KindEnd, count, nil, 0))
	expectFailed(t, l.last(count), frpc.CodeInvalidArgument, "callee END carrying FRpcError's id")

	upload := l.openPlain(demo.UploadOpenID, demo.EncodeUploadOpen("u"))
	l.pump(4)
	l.toCallee(demo.UploadChunkID, frame(frpc.KindEnd, upload, nil, 0))
	answer := only(t, l.drainCallee())
	expectEqual(t, frpc.KindError, answer.kind(), "the callee ends the call")
	expectEqual(t, frpc.CodeInvalidArgument, answer.code(), "caller END with a model id")
}

func TestCancelWithAnIDOtherThanVoidIsInvalidArgument(t *testing.T) {
	l := newLink(t, linkSetup{caller: slowCaller(t), callee: slowCallee(t, nil)})
	call := l.openPlain(slow.RequestID, demo.EncodeEcho("x"))
	l.pump(4)
	l.toCallee(demo.EchoRequestID, frame(frpc.KindCancel, call, nil, 0))
	expectEqual(t, frpc.CodeInvalidArgument, only(t, l.drainCallee()).code(), "code")
}

func TestAFailingHandlerBecomesInternalForItsCallOnly(t *testing.T) {
	callee := build(t, demo.Builder().
		Serve(demo.Say, func(frpc.Method, []byte) (frpc.Handler, error) { return nil, errors.New("boom") }).
		Serve(demo.Count, func(_ frpc.Method, body []byte) (frpc.Handler, error) {
			return frpc.ItemsOf([][]byte{body}), nil
		}))
	l := newLink(t, linkSetup{callee: callee})
	broken := l.openPlain(demo.EchoRequestID, demo.EncodeEcho("x"))
	fine := l.openPlain(demo.CountRequestID, demo.EncodeCount(1))
	l.pump(4)
	expectEqual(t, "boom", expectFailed(t, l.last(broken), frpc.CodeInternal, "returned").Message, "message")
	expectEqual(t, frpc.OutcomeEnd, l.last(fine).Kind, "the other call completes")
}

func TestAPanickingHandlerBecomesInternalForItsCallOnly(t *testing.T) {
	callee := build(t, demo.Builder().
		Serve(demo.Say, serving(frpc.PollFunc(func(time.Time) frpc.Progress { panic("boom") }))).
		Serve(demo.Count, func(frpc.Method, []byte) (frpc.Handler, error) { return frpc.ItemsOf(nil), nil }))
	l := newLink(t, linkSetup{callee: callee})
	broken := l.openPlain(demo.EchoRequestID, demo.EncodeEcho("x"))
	fine := l.openPlain(demo.CountRequestID, demo.EncodeCount(1))
	l.pump(4)
	expectEqual(t, "boom", expectFailed(t, l.last(broken), frpc.CodeInternal, "panicked").Message, "message")
	expectEqual(t, frpc.OutcomeEnd, l.last(fine).Kind, "the other call completes")
}

func TestAFullDeadlineCycleRunsOnInjectedTime(t *testing.T) {
	handler := &forever{}
	l := newLink(t, linkSetup{caller: slowCaller(t), callee: slowCallee(t, func() frpc.Handler { return handler })})
	call := l.open(slow.RequestID, demo.EncodeEcho("x"), nil, frpc.Within(100*time.Millisecond))
	l.pump(4)
	expectEqual(t, 0, len(l.forCall(call)), "still waiting")

	l.now = l.now.Add(100 * time.Millisecond)
	l.caller.Tick(l.now)
	l.outcomes = append(l.outcomes, l.caller.TakeOutcomes()...)
	expectFailed(t, l.last(call), frpc.CodeDeadlineExceeded, "expired")
	cancel := only(t, l.drainCaller())
	expectEqual(t, frpc.KindCancel, cancel.kind(), "CANCEL follows the completion")

	l.toCallee(cancel.messageID, cancel.payload)
	expectTrue(t, handler.cancelled, "the handler hears the cancellation")
	expectEqual(t, frpc.KindError, only(t, l.drainCallee()).kind(), "the callee answers CANCELLED")
}

func TestAResponseInTheSameTickAsTheDeadlineWins(t *testing.T) {
	l := newLink(t, linkSetup{})
	l.open(demo.EchoRequestID, demo.EncodeEcho("x"), nil, frpc.Within(50*time.Millisecond))
	request := only(t, l.drainCaller())
	l.toCallee(request.messageID, request.payload)
	response := only(t, l.drainCallee())
	l.now = l.now.Add(80 * time.Millisecond)
	l.caller.OnMessage(response.messageID, response.payload, l.now)
	l.caller.Tick(l.now)
	outcomes := l.caller.TakeOutcomes()
	expectEqual(t, 1, len(outcomes), "one completion")
	expectEqual(t, frpc.OutcomeResponse, outcomes[0].Kind, "the answer already in the buffer wins")
}

func TestAUnaryHandlerIsEndedByItsOwnLimitWhenCancelNeverArrives(t *testing.T) {
	handler := &forever{}
	l := newLink(t, linkSetup{
		caller:       slowCaller(t),
		callee:       slowCallee(t, func() frpc.Handler { return handler }),
		calleeConfig: frpc.Config{HandlerLimit: time.Second},
	})
	call := l.openPlain(slow.RequestID, demo.EncodeEcho("x"))
	l.pump(4)
	l.now = l.now.Add(time.Second)
	l.pump(4)
	expectFailed(t, l.last(call), frpc.CodeDeadlineExceeded, "the callee's own limit")
	expectTrue(t, handler.cancelled, "the handler was told")
}

func TestAStreamWithNoDeadlineOutlivesTheUnaryLimit(t *testing.T) {
	l := newLink(t, linkSetup{caller: slowCaller(t), callee: slowCallee(t, nil), calleeConfig: frpc.Config{HandlerLimit: time.Second}})
	call := l.openPlain(demo.UploadOpenID, demo.EncodeUploadOpen("u"))
	l.pump(4)
	l.now = l.now.Add(10 * time.Minute)
	l.pump(4)
	expectEqual(t, 0, len(l.forCall(call)), "no outcome")
	expectEqual(t, 1, l.callee.InflightCount(), "the stream is still open")
}

func TestAnIdleLimitEndsAStreamThatHearsNothing(t *testing.T) {
	idle := demo.Upload.IdleAfter(2 * time.Second)
	callee := build(t, demo.Builder().Serve(idle, func(frpc.Method, []byte) (frpc.Handler, error) { return &forever{}, nil }))
	l := newLink(t, linkSetup{caller: slowCaller(t), callee: callee})
	call := l.openPlain(demo.UploadOpenID, demo.EncodeUploadOpen("u"))
	l.pump(4)
	l.now = l.now.Add(time.Second)
	_ = l.caller.SendItem(call, demo.EncodeChunk(1))
	l.pump(4)
	l.now = l.now.Add(1500 * time.Millisecond)
	l.pump(4)
	expectEqual(t, 0, len(l.forCall(call)), "an item reset the idle clock")
	l.now = l.now.Add(2 * time.Second)
	l.pump(4)
	expectFailed(t, l.last(call), frpc.CodeDeadlineExceeded, "silent past the idle limit")
}

func TestCreditCountsAsActivityForTheIdleLimit(t *testing.T) {
	idle := demo.Chat.IdleAfter(2 * time.Second)
	callee := build(t, demo.Builder().Serve(idle, func(frpc.Method, []byte) (frpc.Handler, error) { return &forever{}, nil }))
	l := newLink(t, linkSetup{caller: build(t, demo.Builder().Declare(demo.Chat)), callee: callee})
	call := l.openPlain(demo.ChatOpenID, demo.EncodeRoom("r"))
	l.pump(4)
	for range 4 {
		l.now = l.now.Add(1500 * time.Millisecond)
		_ = l.caller.Grant(call, 1)
		l.pump(4)
	}
	expectEqual(t, 0, len(l.forCall(call)), "CREDIT kept it alive")
}

func TestNPendingCallsCompleteExactlyNTimesWithUnavailable(t *testing.T) {
	l := newLink(t, linkSetup{caller: slowCaller(t), callee: slowCallee(t, nil)})
	for range 3 {
		l.openPlain(slow.RequestID, demo.EncodeEcho("x"))
	}
	l.pump(4)
	l.caller.OnSessionEnd()
	outcomes := l.caller.TakeOutcomes()
	expectEqual(t, 3, len(outcomes), "one completion per call")
	for _, outcome := range outcomes {
		expectFailed(t, outcome, frpc.CodeUnavailable, "each completion")
	}
	expectEqual(t, 0, len(l.drainCaller()), "nothing is sent")
}

func TestASecondTerminationPathCleansNothingAgain(t *testing.T) {
	l := newLink(t, linkSetup{caller: slowCaller(t), callee: slowCallee(t, nil)})
	l.openPlain(slow.RequestID, demo.EncodeEcho("x"))
	l.pump(4)
	l.caller.OnSessionEnd()
	l.caller.OnSessionEnd()
	expectEqual(t, 1, len(l.caller.TakeOutcomes()), "exactly one completion")
}

func TestARunningHandlerIsCancelledAndSendsNothing(t *testing.T) {
	handler := &forever{}
	l := newLink(t, linkSetup{caller: slowCaller(t), callee: slowCallee(t, func() frpc.Handler { return handler })})
	l.openPlain(slow.RequestID, demo.EncodeEcho("x"))
	l.pump(4)
	l.callee.OnSessionEnd()
	expectTrue(t, handler.cancelled, "cancellation signal")
	expectEqual(t, 0, len(l.drainCallee()), "no frame after the session ended")
}
