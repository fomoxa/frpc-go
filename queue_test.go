package frpc_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	frpc "github.com/fomoxa/frpc-go"
	"github.com/fomoxa/frpc-go/examples/demo"
)

func TestACallQueuedBehindALongStreamDoesNotWaitForAllOfIt(t *testing.T) {
	l := newLink(t, linkSetup{callerConfig: frpc.Config{SchedulerQuantum: 32, MaxCallQueue: 2000, MaxConnectionQueue: 4000}})
	big := l.openPlain(demo.UploadOpenID, demo.EncodeUploadOpen("big"))
	for range 1000 {
		expectTrue(t, l.caller.SendItem(big, demo.EncodeChunk(1)) == nil, "an item is queued")
	}
	small := l.openPlain(demo.EchoRequestID, demo.EncodeEcho("small"))
	sent := l.drainCaller()
	smallAt, lastBig := -1, -1
	for index, frame := range sent {
		if frame.call() == small && smallAt < 0 {
			smallAt = index
		}
		if frame.call() == big {
			lastBig = index
		}
	}
	expectEqual(t, 1001, countFor(sent, big), "the stream's REQUEST and 1,000 items all went out")
	expectTrue(t, smallAt < lastBig, "the small REQUEST went out before the stream's last frame")
	expectTrue(t, smallAt < 10, "the small REQUEST waited for a few frames, not for the stream")
}

func countFor(frames []sent, call frpc.CallID) int {
	count := 0
	for _, frame := range frames {
		if frame.call() == call {
			count++
		}
	}
	return count
}

func TestCallsShareTheLinkByBytesNotByFrameCount(t *testing.T) {
	l := newLink(t, linkSetup{callerConfig: frpc.Config{SchedulerQuantum: 1000}})
	large := l.openPlain(demo.ChatOpenID, demo.EncodeRoom("l"))
	small := l.openPlain(demo.UploadOpenID, demo.EncodeUploadOpen("s"))
	for range 6 {
		_ = l.caller.SendItem(large, demo.EncodeSaid(strings.Repeat("L", 600)))
		_ = l.caller.SendItem(small, demo.EncodeChunk(1))
	}
	var order []frpc.CallID
	for _, frame := range l.drainCaller() {
		order = append(order, frame.call())
	}
	firstTurn := 0
	for firstTurn < len(order) && order[firstTurn] == large {
		firstTurn++
	}
	secondTurn := 0
	for firstTurn+secondTurn < len(order) && order[firstTurn+secondTurn] == small {
		secondTurn++
	}
	expectEqual(t, 3, firstTurn, "two 600-byte items exhaust the large call's quantum")
	expectEqual(t, 7, secondTurn, "the small call sends every small frame in one turn")
}

func TestACancelledCallDiscardsItsQueueAndSendsOnlyCancel(t *testing.T) {
	l := newLink(t, linkSetup{})
	call := l.openPlain(demo.UploadOpenID, demo.EncodeUploadOpen("u"))
	l.drainCaller()
	_ = l.caller.SendItem(call, demo.EncodeChunk(1))
	_ = l.caller.SendItem(call, demo.EncodeChunk(2))
	l.caller.Cancel(call)
	expectSequence(t, []frpc.Kind{frpc.KindCancel}, kinds(l.drainCaller()), "only CANCEL")
	outcomes := l.caller.TakeOutcomes()
	expectEqual(t, 1, len(outcomes), "one completion")
	expectFailed(t, outcomes[0], frpc.CodeCancelled, "completed locally")
}

func TestCancellingWhileTheRequestIsQueuedSendsNoBytes(t *testing.T) {
	l := newLink(t, linkSetup{})
	call := l.openPlain(demo.CountRequestID, demo.EncodeCount(3))
	_ = l.caller.Grant(call, 1)
	l.caller.Cancel(call)
	expectEqual(t, 0, len(l.drainCaller()), "no bytes")
	outcomes := l.caller.TakeOutcomes()
	expectEqual(t, 1, len(outcomes), "one completion")
	expectFailed(t, outcomes[0], frpc.CodeCancelled, "completed locally")
}

func TestAFullQueueWaitsBelowTheThresholdAndFailsAboveIt(t *testing.T) {
	l := newLink(t, linkSetup{})
	l.openPlain(demo.TicksRequestID, demo.EncodeTicksRequest())
	for _, frame := range l.drainCaller() {
		l.toCallee(frame.messageID, frame.payload)
	}
	l.now = l.now.Add(4 * time.Second)
	l.callee.Tick(l.now)
	expectEqual(t, 1, l.callee.InflightCount(), "below the threshold the producer waits")
	l.now = l.now.Add(time.Second)
	l.callee.Tick(l.now)
	expectEqual(t, 0, l.callee.InflightCount(), "past five seconds the stream ends")
	answer := only(t, l.drainCallee())
	expectEqual(t, frpc.CodeResourceExhausted, answer.code(), "the queue was discarded")
}

func TestThePerCallCeilingCongestsOnlyThatCall(t *testing.T) {
	l := newLink(t, linkSetup{callerConfig: frpc.Config{MaxCallQueue: 3}})
	first := l.openPlain(demo.UploadOpenID, demo.EncodeUploadOpen("a"))
	_ = l.caller.SendItem(first, demo.EncodeChunk(1))
	_ = l.caller.SendItem(first, demo.EncodeChunk(1))
	expectTrue(t, errors.Is(l.caller.SendItem(first, demo.EncodeChunk(1)), frpc.ErrCongested), "that call is full")
	second := l.openPlain(demo.UploadOpenID, demo.EncodeUploadOpen("b"))
	expectTrue(t, l.caller.SendItem(second, demo.EncodeChunk(1)) == nil, "another call is not")
}

func TestTheConnectionCeilingCongestsSendsAndTheSessionStaysAlive(t *testing.T) {
	l := newLink(t, linkSetup{callerConfig: frpc.Config{MaxConnectionQueue: 3}})
	call := l.openPlain(demo.UploadOpenID, demo.EncodeUploadOpen("a"))
	_ = l.caller.SendItem(call, demo.EncodeChunk(1))
	_ = l.caller.SendItem(call, demo.EncodeChunk(1))
	expectTrue(t, errors.Is(l.caller.SendItem(call, demo.EncodeChunk(1)), frpc.ErrCongested), "item refused")
	_, err := l.caller.Call(demo.EchoRequestID, demo.EncodeEcho("x"), nil, frpc.DefaultCallOptions(), l.now)
	expectTrue(t, errors.Is(err, frpc.ErrCongested), "a new call is refused too")
	l.pump(4)
	expectTrue(t, l.caller.SendItem(call, demo.EncodeChunk(1)) == nil, "room again after a drain")
}

func TestTerminatingFramesAreNeverRefusedByTheConnectionCeiling(t *testing.T) {
	l := newLink(t, linkSetup{caller: slowCaller(t), callee: slowCallee(t, nil), callerConfig: frpc.Config{MaxConnectionQueue: 2}})
	upload := l.openPlain(demo.UploadOpenID, demo.EncodeUploadOpen("u"))
	waiting := l.openPlain(slow.RequestID, demo.EncodeEcho("x"))
	l.pump(4)
	_ = l.caller.SendItem(upload, demo.EncodeChunk(1))
	_ = l.caller.SendItem(upload, demo.EncodeChunk(1))
	expectTrue(t, errors.Is(l.caller.SendItem(upload, demo.EncodeChunk(1)), frpc.ErrCongested), "the connection is full")
	l.caller.Cancel(waiting)
	cancelQueued := false
	for _, frame := range l.drainCaller() {
		cancelQueued = cancelQueued || (frame.call() == waiting && frame.kind() == frpc.KindCancel)
	}
	expectTrue(t, cancelQueued, "CANCEL queued past the ceiling")

	callee := newLink(t, linkSetup{calleeConfig: frpc.Config{MaxConnectionQueue: 2}})
	first := callee.openPlain(demo.TicksRequestID, demo.EncodeTicksRequest())
	second := callee.openPlain(demo.TicksRequestID, demo.EncodeTicksRequest())
	for _, frame := range callee.drainCaller() {
		callee.toCallee(frame.messageID, frame.payload)
	}
	callee.toCallee(demo.VoidID, frame(frpc.KindCancel, second, nil, 0))
	sent := callee.drainCallee()
	expectEqual(t, 2, countFor(sent, first), "items stopped at the ceiling")
	errorQueued := false
	for _, frame := range sent {
		errorQueued = errorQueued || (frame.call() == second && frame.kind() == frpc.KindError)
	}
	expectTrue(t, errorQueued, "ERROR queued past the ceiling")
}

func TestARejectedFrameStaysQueuedAndGoesOutOnTheNextDrain(t *testing.T) {
	l := newLink(t, linkSetup{})
	l.openPlain(demo.EchoRequestID, demo.EncodeEcho("x"))
	l.caller.Drain(func(frpc.Outgoing) frpc.Delivery { return frpc.Rejected })
	expectEqual(t, 1, l.caller.QueuedFrames(), "kept")
	accepted := 0
	l.caller.Drain(func(frpc.Outgoing) frpc.Delivery {
		accepted++
		return frpc.Accepted
	})
	expectEqual(t, 1, accepted, "sent once net takes it")
}
