package frpc_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	frpc "github.com/fomoxa/frpc-go"
	"github.com/fomoxa/frpc-go/examples/demo"
)

func ticksOf(l *link, call frpc.CallID) []uint32 {
	var ticks []uint32
	for _, outcome := range l.forCall(call) {
		if outcome.Kind == frpc.OutcomeItem {
			ticks = append(ticks, demo.DecodeTick(outcome.Body))
		}
	}
	return ticks
}

func itemCount(outcomes []frpc.Outcome) int {
	count := 0
	for _, outcome := range outcomes {
		if outcome.Kind == frpc.OutcomeItem {
			count++
		}
	}
	return count
}

func TestServerStreamItemsInOrderThenEnd(t *testing.T) {
	l := newLink(t, linkSetup{})
	call := l.openPlain(demo.CountRequestID, demo.EncodeCount(3))
	l.pump(4)
	seen := l.forCall(call)
	var values []uint32
	for _, outcome := range seen[:3] {
		values = append(values, demo.DecodeCountItem(outcome.Body))
	}
	expectSequence(t, []uint32{1, 2, 3}, values, "items")
	expectEqual(t, frpc.OutcomeEnd, seen[len(seen)-1].Kind, "END")
}

func TestClientStreamItemsReachTheHandlerInOrderAndRespondOnlyAfterEnd(t *testing.T) {
	handler := &recording{}
	l := newLink(t, linkSetup{callee: build(t, demo.Builder().Serve(demo.Upload, serving(handler)))})
	call := l.openPlain(demo.UploadOpenID, demo.EncodeUploadOpen("u"))
	for _, size := range []uint32{5, 6, 7} {
		expectTrue(t, l.caller.SendItem(call, demo.EncodeChunk(size)) == nil, "queued")
		l.pump(4)
	}
	expectEqual(t, 0, len(l.forCall(call)), "no RESPONSE before END")
	_ = l.caller.CloseSend(call)
	l.pump(4)
	expectSequence(t, []uint32{5, 6, 7}, handler.sizes, "handler order")
	expectEqual(t, "3 items", demo.DecodeDone(l.last(call).Body), "the RESPONSE")
}

func TestACallerItemWithTheWrongMessageIDIsInvalidArgument(t *testing.T) {
	l := newLink(t, linkSetup{})
	call := l.openPlain(demo.UploadOpenID, demo.EncodeUploadOpen("u"))
	l.pump(4)
	l.toCallee(demo.ChatSaidID, frame(frpc.KindItem, call, demo.EncodeSaid("x"), 0))
	for _, frame := range l.drainCallee() {
		l.toCaller(frame.messageID, frame.payload)
	}
	expectFailed(t, l.last(call), frpc.CodeInvalidArgument, "wrong caller item type")
	expectEqual(t, 0, l.callee.InflightCount(), "the call terminated")
}

func TestAnItemAfterTheCallerDirectionClosedIsALocalErrorWithNoBytes(t *testing.T) {
	l := newLink(t, linkSetup{})
	call := l.openPlain(demo.UploadOpenID, demo.EncodeUploadOpen("u"))
	_ = l.caller.CloseSend(call)
	l.drainCaller()
	expectTrue(t, errors.Is(l.caller.SendItem(call, demo.EncodeChunk(1)), frpc.ErrSendClosed), "refused")
	expectEqual(t, 0, len(l.drainCaller()), "no bytes")
}

func TestAnItemAfterEndIsIgnored(t *testing.T) {
	l := newLink(t, linkSetup{})
	call := l.openPlain(demo.CountRequestID, demo.EncodeCount(1))
	l.pump(4)
	seen := len(l.forCall(call))
	l.toCaller(demo.CountItemID, frame(frpc.KindItem, call, demo.EncodeCountItem(9), 0))
	expectEqual(t, seen, len(l.forCall(call)), "nothing more")
}

func TestBidiDirectionsInterleaveAndEachEndIsIndependent(t *testing.T) {
	l := newLink(t, linkSetup{})
	call := l.openPlain(demo.ChatOpenID, demo.EncodeRoom("r"))
	var heard []string
	for _, line := range []string{"a", "b"} {
		_ = l.caller.SendItem(call, demo.EncodeSaid(line))
		l.pump(4)
		heard = append(heard, demo.DecodeHeard(l.last(call).Body))
	}
	expectSequence(t, []string{"A", "B"}, heard, "replies interleave with sends")
	_ = l.caller.CloseSend(call)
	l.pump(4)
	expectEqual(t, frpc.OutcomeEnd, l.last(call).Kind, "the callee's END follows the caller's")
	count := func(side frpc.Sender, kind frpc.Kind) int {
		total := 0
		for _, frame := range l.sentBy(side) {
			if frame.kind() == kind {
				total++
			}
		}
		return total
	}
	expectEqual(t, 2, count(frpc.SenderCaller, frpc.KindItem), "caller items")
	expectEqual(t, 1, count(frpc.SenderCaller, frpc.KindEnd), "one caller END")
	expectEqual(t, 1, count(frpc.SenderCallee, frpc.KindEnd), "one callee END")
}

func TestZeroInitialCreditLeavesTheReplyDirectionUnbounded(t *testing.T) {
	l := newLink(t, linkSetup{})
	call := l.open(demo.CountRequestID, demo.EncodeCount(20), &frpc.Ctx{InitialCredit: 0, Tenant: "t"}, frpc.WithoutDeadline())
	l.pump(8)
	expectEqual(t, 20, itemCount(l.forCall(call)), "every item")
}

func TestInitialCreditProducesExactlyNAndKeepsTheCallOpen(t *testing.T) {
	l := newLink(t, linkSetup{})
	call := l.open(demo.TicksRequestID, demo.EncodeTicksRequest(), frpc.WithInitialCredit(3), frpc.WithoutDeadline())
	l.pump(8)
	expectSequence(t, []uint32{1, 2, 3}, ticksOf(l, call), "exactly three")
	expectEqual(t, 1, l.callee.InflightCount(), "the call stays open")
}

func TestCreditReleasesExactlyTheGrantedItemsNumberedOn(t *testing.T) {
	l := newLink(t, linkSetup{})
	call := l.open(demo.TicksRequestID, demo.EncodeTicksRequest(), frpc.WithInitialCredit(2), frpc.WithoutDeadline())
	l.pump(4)
	_ = l.caller.Grant(call, 3)
	l.pump(8)
	expectSequence(t, []uint32{1, 2, 3, 4, 5}, ticksOf(l, call), "two, then three more")
}

func TestWithoutInitialCreditTheFirstCreditEstablishesTheAllowance(t *testing.T) {
	l := newLink(t, linkSetup{calleeConfig: frpc.Config{MaxCallQueue: 4}})
	call := l.openPlain(demo.TicksRequestID, demo.EncodeTicksRequest())
	_ = l.caller.Grant(call, 2)
	l.pump(8)
	expectSequence(t, []uint32{1, 2, 3, 4, 5, 6}, ticksOf(l, call), "a queue's worth before the CREDIT, then two")
}

func TestACalleeCreditInTheRequestTickBoundsTheCallerDirection(t *testing.T) {
	handler := &recording{grant: 2}
	l := newLink(t, linkSetup{callee: build(t, demo.Builder().Serve(demo.Upload, serving(handler)))})
	call := l.openPlain(demo.UploadOpenID, demo.EncodeUploadOpen("u"))
	l.pump(4)
	expectTrue(t, l.caller.SendItem(call, demo.EncodeChunk(1)) == nil, "first")
	expectTrue(t, l.caller.SendItem(call, demo.EncodeChunk(2)) == nil, "second")
	expectTrue(t, errors.Is(l.caller.SendItem(call, demo.EncodeChunk(3)), frpc.ErrNoCredit), "third waits")
}

func TestItemsBeyondTheAllowanceAreDeliveredAsOrdinaryItems(t *testing.T) {
	handler := &recording{grant: 1}
	l := newLink(t, linkSetup{callee: build(t, demo.Builder().Serve(demo.Upload, serving(handler)))})
	call := l.openPlain(demo.UploadOpenID, demo.EncodeUploadOpen("u"))
	for _, size := range []uint32{1, 2, 3} {
		_ = l.caller.SendItem(call, demo.EncodeChunk(size))
	}
	l.pump(4)
	expectSequence(t, []uint32{1, 2, 3}, handler.sizes, "all three reach the handler")
}

func TestCreditForAClosedCallIsIgnored(t *testing.T) {
	l := newLink(t, linkSetup{})
	l.toCallee(demo.CreditID, frame(frpc.KindCredit, 44, credit(5), 0))
	l.toCaller(demo.CreditID, frame(frpc.KindCredit, 44, credit(5), 0))
	expectEqual(t, 0, len(l.drainCallee())+len(l.drainCaller())+len(l.outcomes), "ignored")
}

func TestASustainedZeroAllowanceNeverTripsResourceExhausted(t *testing.T) {
	l := newLink(t, linkSetup{})
	call := l.open(demo.TicksRequestID, demo.EncodeTicksRequest(), frpc.WithInitialCredit(1), frpc.WithoutDeadline())
	l.pump(4)
	l.now = l.now.Add(30 * time.Second)
	l.pump(4)
	expectEqual(t, 1, len(l.forCall(call)), "one item and no failure")
	expectEqual(t, 1, l.callee.InflightCount(), "still open")
}

func TestAPeerWithoutCreditIgnoresCreditAndTheApplicationSeesNothing(t *testing.T) {
	callee := build(t, frpc.NewRegistry(demo.VoidID, demo.ErrorID).Serve(demo.Count,
		func(frpc.Method, []byte) (frpc.Handler, error) { return &numbers{count: 5}, nil }))
	l := newLink(t, linkSetup{callee: callee})
	call := l.openPlain(demo.CountRequestID, demo.EncodeCount(5))
	_ = l.caller.Grant(call, 1)
	l.pump(4)
	expectTrue(t, l.toCallee(demo.CreditID, frame(frpc.KindCredit, call, credit(1), 0)), "not for the application")
	expectEqual(t, 5, itemCount(l.forCall(call)), "unbounded")
	expectEqual(t, frpc.OutcomeEnd, l.last(call).Kind, "and it completes")
}

func TestCreditCarryingAnotherMessageIDIsInvalidArgument(t *testing.T) {
	l := newLink(t, linkSetup{})
	call := l.open(demo.TicksRequestID, demo.EncodeTicksRequest(), frpc.WithInitialCredit(1), frpc.WithoutDeadline())
	l.pump(4)
	l.toCallee(demo.VoidID, frame(frpc.KindCredit, call, credit(1), 0))
	expectEqual(t, frpc.CodeInvalidArgument, only(t, l.drainCallee()).code(), "code")

	fine := l.openPlain(demo.EchoRequestID, demo.EncodeEcho("x"))
	l.pump(4)
	expectEqual(t, frpc.OutcomeResponse, l.last(fine).Kind, "the session stays alive")
}

func TestGrantsOnAFullCallNeverCongestAndCoalesceIntoOneCredit(t *testing.T) {
	l := newLink(t, linkSetup{callerConfig: frpc.Config{MaxCallQueue: 1}})
	call := l.openPlain(demo.ChatOpenID, demo.EncodeRoom("r"))
	l.drainCaller()
	_ = l.caller.SendItem(call, demo.EncodeSaid("fills the queue"))
	expectTrue(t, errors.Is(l.caller.SendItem(call, demo.EncodeSaid("x")), frpc.ErrCongested), "the call is full")
	expectTrue(t, l.caller.Grant(call, 2) == nil, "first grant")
	expectTrue(t, l.caller.Grant(call, 3) == nil, "second grant")
	frames := l.drainCaller()
	var credits []sent
	for _, frame := range frames {
		if frame.kind() == frpc.KindCredit {
			credits = append(credits, frame)
		}
	}
	expectEqual(t, 1, len(credits), "one CREDIT")
	expectBytes(t, credit(5), credits[0].payload[5:], "carrying the sum")
	expectEqual(t, frpc.KindCredit, frames[0].kind(), "CREDIT goes ahead of the queued ITEM")
}

func TestCreditNeverOvertakesItsOwnRequest(t *testing.T) {
	l := newLink(t, linkSetup{})
	call := l.openPlain(demo.CountRequestID, demo.EncodeCount(1))
	_ = l.caller.Grant(call, 4)
	expectSequence(t, []frpc.Kind{frpc.KindRequest, frpc.KindCredit}, kinds(l.drainCaller()), "REQUEST first")
}

type recording struct {
	sizes  []uint32
	grant  uint32
	closed bool
}

func (h *recording) Poll(time.Time) frpc.Progress {
	if h.closed {
		return frpc.Respond(demo.EncodeDone(fmt.Sprintf("%d items", len(h.sizes))))
	}
	return frpc.Pending()
}

func (h *recording) Item(body []byte) error {
	h.sizes = append(h.sizes, demo.DecodeChunk(body))
	return nil
}

func (h *recording) EndOfStream() error {
	h.closed = true
	return nil
}

func (h *recording) Grant() uint32 {
	granted := h.grant
	h.grant = 0
	return granted
}

type numbers struct {
	count uint32
	next  uint32
}

func (h *numbers) Poll(time.Time) frpc.Progress {
	if h.next >= h.count {
		return frpc.End()
	}
	h.next++
	return frpc.Item(demo.EncodeCountItem(h.next))
}
