package frpc

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"time"
)

const MaxBodyCeiling = 16*1024*1024 - HeaderLen

type Config struct {
	MaxPending         int
	MaxCallQueue       int
	MaxConnectionQueue int
	SchedulerQuantum   int
	MaxCtxLen          int
	MaxBodyLen         int
	HandlerLimit       time.Duration
	QueueFullLimit     time.Duration
	CompressAbove      int
}

func DefaultConfig() Config {
	return Config{
		MaxPending:         65_536,
		MaxCallQueue:       64,
		MaxConnectionQueue: 1_024,
		SchedulerQuantum:   64 * 1024,
		MaxCtxLen:          64 * 1024,
		MaxBodyLen:         MaxBodyCeiling,
		HandlerLimit:       30 * time.Second,
		QueueFullLimit:     5 * time.Second,
		CompressAbove:      1024,
	}
}

func (c Config) normalized() Config {
	defaults := DefaultConfig()
	if c.MaxPending <= 0 {
		c.MaxPending = defaults.MaxPending
	}
	if c.MaxCallQueue <= 0 {
		c.MaxCallQueue = defaults.MaxCallQueue
	}
	if c.MaxConnectionQueue <= 0 {
		c.MaxConnectionQueue = defaults.MaxConnectionQueue
	}
	if c.SchedulerQuantum <= 0 {
		c.SchedulerQuantum = defaults.SchedulerQuantum
	}
	if c.MaxCtxLen <= 0 {
		c.MaxCtxLen = defaults.MaxCtxLen
	}
	if c.MaxBodyLen <= 0 || c.MaxBodyLen > MaxBodyCeiling {
		c.MaxBodyLen = defaults.MaxBodyLen
	}
	if c.HandlerLimit <= 0 {
		c.HandlerLimit = defaults.HandlerLimit
	}
	if c.QueueFullLimit <= 0 {
		c.QueueFullLimit = defaults.QueueFullLimit
	}
	if c.CompressAbove <= 0 {
		c.CompressAbove = defaults.CompressAbove
	}
	return c
}

var (
	ErrNotReady          = errors.New("frpc: the session is not ready")
	ErrUnknownMethod     = errors.New("frpc: no method is declared for this request type")
	ErrUnknownCall       = errors.New("frpc: no such call")
	ErrWrongShape        = errors.New("frpc: the method's shape does not allow this")
	ErrSendClosed        = errors.New("frpc: the caller direction is already closed")
	ErrNoCredit          = errors.New("frpc: the allowance for this direction is exhausted")
	ErrCreditUnsupported = errors.New("frpc: FRpcCredit is not declared")
	ErrTooManyPending    = errors.New("frpc: the pending ceiling is reached")
	ErrCongested         = errors.New("frpc: the send queue is full")
	ErrBodyTooLarge      = errors.New("frpc: the body exceeds the ceiling")
	ErrCtxTooLarge       = errors.New("frpc: the ctx block exceeds the ceiling")
	ErrCallIDExhausted   = errors.New("frpc: no call id is free")
)

type OutcomeKind int

const (
	OutcomeResponse OutcomeKind = iota
	OutcomeItem
	OutcomeEnd
	OutcomeFailed
)

func (k OutcomeKind) String() string {
	switch k {
	case OutcomeResponse:
		return "response"
	case OutcomeItem:
		return "item"
	case OutcomeEnd:
		return "end"
	default:
		return "failed"
	}
}

type Outcome struct {
	Kind   OutcomeKind
	Call   CallID
	Body   []byte
	Status *Status
}

func (o Outcome) Terminates() bool { return o.Kind != OutcomeItem }

func (o Outcome) String() string {
	switch o.Kind {
	case OutcomeFailed:
		return fmt.Sprintf("%s failed: %s", o.Call, o.Status)
	case OutcomeEnd:
		return fmt.Sprintf("%s ended", o.Call)
	default:
		return fmt.Sprintf("%s %s of %d bytes", o.Call, o.Kind, len(o.Body))
	}
}

type CallOptions struct {
	Deadline time.Duration
}

func DefaultCallOptions() CallOptions { return CallOptions{Deadline: 5 * time.Second} }

func Within(deadline time.Duration) CallOptions { return CallOptions{Deadline: deadline} }

func WithoutDeadline() CallOptions { return CallOptions{} }

type waiting struct {
	context     *CallContext
	entered     int
	requestID   uint32
	itemID      uint32
	replyID     uint32
	shape       Shape
	deadline    time.Time
	sendClosed  bool
	credit      uint32
	established bool
}

type running struct {
	handler       Handler
	itemID        uint32
	replyID       uint32
	shape         Shape
	limit         time.Time
	idleLimit     time.Duration
	lastHeard     time.Time
	inboundClosed bool
	credit        uint32
	established   bool
	fullSince     time.Time
	context       *CallContext
	entered       int
}

func (r *running) starved() bool { return r.established && r.credit == 0 }

func (r *running) nextExpiry() time.Time {
	var idle time.Time
	if r.idleLimit > 0 {
		idle = r.lastHeard.Add(r.idleLimit)
	}
	return earlier(r.limit, idle)
}

type Core struct {
	registry   *Registry
	config     Config
	ids        *CallIDAllocator
	pending    map[uint32]*waiting
	inflight   map[uint32]*running
	outbox     *Outbox
	outcomes   []Outcome
	chain      Chain
	compressor Compressor
	identity   *Identity
	ready      bool
	finished   bool
}

func NewCore(registry *Registry, role Role, config Config) *Core {
	config = config.normalized()
	return &Core{
		registry: registry,
		config:   config,
		ids:      NewCallIDAllocator(role),
		pending:  map[uint32]*waiting{},
		inflight: map[uint32]*running{},
		outbox:   NewOutbox(config.MaxCallQueue, config.MaxConnectionQueue, config.SchedulerQuantum),
	}
}

func (c *Core) Identity() *Identity { return c.identity }

func (c *Core) Role() Role { return c.ids.Role() }

func (c *Core) Registry() *Registry { return c.registry }

func (c *Core) Config() Config { return c.config }

func (c *Core) Ready() bool { return c.ready }

func (c *Core) PendingCount() int { return len(c.pending) }

func (c *Core) InflightCount() int { return len(c.inflight) }

func (c *Core) QueuedFrames() int { return c.outbox.Len() }

func (c *Core) HasUnsentFrames() bool { return !c.outbox.Empty() }

func (c *Core) SetCompressor(compressor Compressor) { c.compressor = compressor }

func (c *Core) AddInterceptor(link Interceptor) { c.chain.Add(link) }

func (c *Core) Authenticate(identity *Identity) { c.identity = identity }

func (c *Core) ClearIdentity() { c.identity = nil }

func (c *Core) PendingRequestID(call CallID) (uint32, bool) {
	record, ok := c.pending[uint32(call)]
	if !ok {
		return 0, false
	}
	return record.requestID, true
}

func (c *Core) WaitingCredit(call CallID) (uint32, bool) { return c.outbox.WaitingCredit(call) }

func (c *Core) NextDeadline() (time.Time, bool) {
	var nearest time.Time
	for _, record := range c.pending {
		nearest = earlier(nearest, record.deadline)
	}
	for _, record := range c.inflight {
		nearest = earlier(nearest, record.nextExpiry())
	}
	return nearest, !nearest.IsZero()
}

func (c *Core) HasRunnableWork() bool {
	for call, record := range c.inflight {
		if !(record.shape.CalleeStreams() && record.starved()) && c.outbox.HasRoom(CallID(call)) {
			return true
		}
	}
	return false
}

func (c *Core) OnReady() { c.ready = true }

func (c *Core) Call(requestID uint32, body []byte, ctx *Ctx, options CallOptions, now time.Time) (CallID, error) {
	if !c.ready {
		return 0, ErrNotReady
	}
	method, ok := c.registry.MethodFor(requestID)
	if !ok {
		return 0, ErrUnknownMethod
	}
	if len(body) > c.config.MaxBodyLen {
		return 0, ErrBodyTooLarge
	}
	if len(c.pending) >= c.config.MaxPending {
		return 0, ErrTooManyPending
	}
	call, ok := c.ids.Allocate(func(candidate CallID) bool {
		_, taken := c.pending[uint32(candidate)]
		return taken
	})
	if !ok {
		return 0, ErrCallIDExhausted
	}

	var outgoing Ctx
	if ctx != nil {
		outgoing = *ctx
	}
	if options.Deadline > 0 {
		budget := millisOf(options.Deadline)
		if !outgoing.HasDeadline() || budget < outgoing.DeadlineMs {
			outgoing.DeadlineMs = budget
		}
	}

	context := newCallContext(call, method, now, outgoing, c.identity, SenderCaller)
	entered, rejection := c.chain.Outgoing(context)
	if rejection != nil {
		c.chain.Outbound(entered, context, rejection)
		return 0, rejection
	}
	var deadline time.Time
	if context.Ctx.HasDeadline() {
		deadline = now.Add(time.Duration(context.Ctx.DeadlineMs) * time.Millisecond)
	}
	context.Deadline = deadline

	header := Header{Kind: KindRequest, Call: call, HasCtx: !context.Ctx.IsEmpty()}
	var encodedCtx []byte
	if header.HasCtx {
		encodedCtx = context.Ctx.Encode()
		if len(encodedCtx) > c.config.MaxCtxLen {
			c.chain.Outbound(entered, context, InvalidArgument("the ctx block exceeds the ceiling"))
			return 0, ErrCtxTooLarge
		}
	}

	sent, header := c.maybeCompress(body, header)
	if !c.outbox.Push(call, Outgoing{MessageID: requestID, Payload: Encode(header, encodedCtx, sent)}) {
		c.chain.Outbound(entered, context, ResourceExhausted("the send queue is full"))
		return 0, ErrCongested
	}

	c.pending[uint32(call)] = &waiting{
		context:   context,
		entered:   entered,
		requestID: method.RequestID,
		itemID:    method.CallerItemID(),
		replyID:   method.ReplyID,
		shape:     method.Shape,
		deadline:  deadline,
	}
	return call, nil
}

func (c *Core) SendItem(call CallID, body []byte) error {
	record, ok := c.pending[uint32(call)]
	if !ok {
		return ErrUnknownCall
	}
	if !record.shape.CallerStreams() {
		return ErrWrongShape
	}
	if record.sendClosed {
		return ErrSendClosed
	}
	if record.established && record.credit == 0 {
		return ErrNoCredit
	}
	if len(body) > c.config.MaxBodyLen {
		return ErrBodyTooLarge
	}
	if !c.outbox.Push(call, c.bodyFrame(call, KindItem, record.itemID, body)) {
		return ErrCongested
	}
	if record.established {
		record.credit--
	}
	return nil
}

func (c *Core) CloseSend(call CallID) error {
	record, ok := c.pending[uint32(call)]
	if !ok {
		return ErrUnknownCall
	}
	if !record.shape.CallerStreams() {
		return ErrWrongShape
	}
	if record.sendClosed {
		return ErrSendClosed
	}
	if !c.outbox.Push(call, c.controlFrame(call, KindEnd)) {
		return ErrCongested
	}
	record.sendClosed = true
	return nil
}

func (c *Core) Grant(call CallID, items uint32) error {
	creditID, supported := c.registry.CreditID()
	if !supported {
		return ErrCreditUnsupported
	}
	if record, ok := c.inflight[uint32(call)]; ok {
		if !record.shape.CallerStreams() {
			return ErrWrongShape
		}
	} else if record, ok := c.pending[uint32(call)]; ok {
		if !record.shape.CalleeStreams() {
			return ErrWrongShape
		}
	} else {
		return ErrUnknownCall
	}
	c.outbox.Grant(call, creditID, items)
	return nil
}

func (c *Core) Cancel(call CallID) {
	if _, ok := c.pending[uint32(call)]; ok {
		c.abandonPending(call, Cancelled())
	}
}

func (c *Core) OnMessage(messageID uint32, payload []byte, now time.Time) bool {
	if c.registry.IsPlain(messageID) {
		return false
	}
	parsed, wireErr := Decode(payload, c.config.MaxCtxLen)
	if wireErr != WireOK {
		if c.registry.Owns(messageID) {
			c.rejectUndecodable(payload, wireErr)
		}
		return true
	}
	if !c.ready {
		return true
	}

	call := parsed.Header.Call
	if parsed.Header.Kind != KindRequest {
		body, failure := c.inflate(parsed.Header, parsed.Body)
		if failure != nil {
			c.failReceived(call, failure)
			return true
		}
		parsed.Body = body
	}

	switch parsed.Header.Kind {
	case KindRequest:
		c.onRequest(messageID, parsed, now)
	case KindResponse:
		c.onResponse(messageID, call, parsed.Body)
	case KindError:
		c.onError(messageID, call, parsed.Body)
	case KindItem:
		c.onItem(messageID, call, parsed.Body, now)
	case KindEnd:
		c.onEnd(messageID, call, now)
	case KindCancel:
		c.onCancel(messageID, call)
	case KindCredit:
		c.onCredit(messageID, call, parsed.Body, now)
	}
	return true
}

func (c *Core) Tick(now time.Time) {
	c.expirePending(now)
	c.expireInflight(now)
	for _, call := range sortedKeys(c.inflight) {
		c.pollOne(CallID(call), now)
	}
}

func (c *Core) Drain(sink Sink) { c.outbox.Drain(sink) }

func (c *Core) OnSessionEnd() {
	if c.finished {
		return
	}
	c.finished = true
	c.ready = false
	for _, call := range sortedKeys(c.pending) {
		c.settlePending(CallID(call), Outcome{Kind: OutcomeFailed, Call: CallID(call), Status: Unavailable()})
	}
	ended := Unavailable()
	abandoned := c.inflight
	c.inflight = map[uint32]*running{}
	for _, call := range sortedKeys(abandoned) {
		record := abandoned[call]
		safeCancel(record.handler)
		c.chain.Outbound(record.entered, record.context, ended)
	}
	c.outbox.Clear()
}

func (c *Core) TakeOutcomes() []Outcome {
	taken := c.outcomes
	c.outcomes = nil
	return taken
}

func (c *Core) onRequest(messageID uint32, parsed Parsed, now time.Time) {
	call := parsed.Header.Call
	if c.ids.Role().Allocates(call) {
		c.failCall(call, InvalidArgument("call id parity belongs to the receiving side"))
		return
	}
	if _, runningAlready := c.inflight[uint32(call)]; runningAlready {
		return
	}
	entry := c.registry.EntryFor(messageID)
	if entry == nil || entry.Factory == nil {
		c.failCall(call, Unimplemented(messageID))
		return
	}
	if len(c.inflight) >= c.config.MaxPending {
		c.failCall(call, ResourceExhausted("too many calls in flight"))
		return
	}

	method := entry.Method
	var ctx Ctx
	if parsed.Header.HasCtx {
		decoded, err := DecodeCtx(parsed.Ctx)
		if err != nil {
			c.failCall(call, InvalidArgument("ctx block failed to decode"))
			return
		}
		ctx = decoded
	}

	var limit time.Time
	if ctx.HasDeadline() {
		limit = now.Add(time.Duration(ctx.DeadlineMs) * time.Millisecond)
	}
	if method.Shape == ShapeUnary {
		own := now.Add(c.config.HandlerLimit)
		if limit.IsZero() || own.Before(limit) {
			limit = own
		}
	}

	context := newCallContext(call, method, now, ctx, c.identity, SenderCallee)
	context.Deadline = limit
	entered, rejection := c.chain.Inbound(context)
	if rejection != nil {
		c.chain.Outbound(entered, context, rejection)
		c.failCall(call, rejection)
		return
	}

	body, failure := c.inflate(parsed.Header, parsed.Body)
	if failure != nil {
		c.chain.Outbound(entered, context, failure)
		c.failCall(call, failure)
		return
	}

	var handler Handler
	failure = guard(func() error {
		var err error
		handler, err = entry.Factory(method, bytes.Clone(body))
		return err
	})
	if failure == nil && handler == nil {
		failure = Internal("the handler factory returned no handler")
	}
	if failure != nil {
		c.chain.Outbound(entered, context, failure)
		c.failCall(call, failure)
		return
	}

	record := &running{
		handler:       handler,
		itemID:        method.CallerItemID(),
		replyID:       method.ReplyID,
		shape:         method.Shape,
		limit:         limit,
		lastHeard:     now,
		inboundClosed: !method.Shape.CallerStreams(),
		context:       context,
		entered:       entered,
	}
	if method.Shape != ShapeUnary {
		record.idleLimit = method.IdleLimit
	}
	if ctx.InitialCredit != 0 {
		record.credit = ctx.InitialCredit
		record.established = true
	}
	c.inflight[uint32(call)] = record
	c.pollOne(call, now)
}

func (c *Core) onResponse(messageID uint32, call CallID, body []byte) {
	record, ok := c.pending[uint32(call)]
	if !ok {
		return
	}
	if record.shape.CalleeStreams() || messageID != record.replyID {
		c.settlePending(call, failed(call, InvalidArgument(fmt.Sprintf(
			"expected response type 0x%08X, received 0x%08X", record.replyID, messageID))))
		return
	}
	c.settlePending(call, Outcome{Kind: OutcomeResponse, Call: call, Body: bytes.Clone(body)})
}

func (c *Core) onError(messageID uint32, call CallID, body []byte) {
	if _, ok := c.pending[uint32(call)]; !ok {
		return
	}
	status := InvalidArgument(fmt.Sprintf("error frame carried 0x%08X, which is not FRpcError", messageID))
	if messageID == c.registry.ErrorID() {
		status = DecodeStatus(body)
	}
	c.settlePending(call, failed(call, status))
}

func (c *Core) onItem(messageID uint32, call CallID, body []byte, now time.Time) {
	if _, ok := c.inflight[uint32(call)]; ok {
		c.onCallerItem(call, messageID, body, now)
		return
	}
	record, ok := c.pending[uint32(call)]
	if !ok {
		return
	}
	if !record.shape.CalleeStreams() || messageID != record.replyID {
		c.abandonPending(call, InvalidArgument(fmt.Sprintf(
			"expected item type 0x%08X, received 0x%08X", record.replyID, messageID)))
		return
	}
	c.outcomes = append(c.outcomes, Outcome{Kind: OutcomeItem, Call: call, Body: bytes.Clone(body)})
}

func (c *Core) onCallerItem(call CallID, messageID uint32, body []byte, now time.Time) {
	record := c.inflight[uint32(call)]
	record.lastHeard = now
	if !record.shape.CallerStreams() {
		c.finishInflight(call, InvalidArgument("this method accepts no caller items"))
		return
	}
	if record.inboundClosed {
		return
	}
	if messageID != record.itemID {
		c.finishInflight(call, InvalidArgument(fmt.Sprintf(
			"expected item type 0x%08X, received 0x%08X", record.itemID, messageID)))
		return
	}
	if refused := deliverItem(record.handler, bytes.Clone(body)); refused != nil {
		c.finishInflight(call, refused)
		return
	}
	c.pollOne(call, now)
}

func (c *Core) onEnd(messageID uint32, call CallID, now time.Time) {
	if record, ok := c.inflight[uint32(call)]; ok {
		record.lastHeard = now
		if messageID != c.registry.VoidID() {
			c.finishInflight(call, InvalidArgument(fmt.Sprintf(
				"end frame carried 0x%08X, which is not FRpcVoid", messageID)))
			return
		}
		if !record.shape.CallerStreams() {
			c.finishInflight(call, InvalidArgument("this method accepts no caller stream"))
			return
		}
		if record.inboundClosed {
			return
		}
		record.inboundClosed = true
		if refused := closeStream(record.handler); refused != nil {
			c.finishInflight(call, refused)
			return
		}
		c.pollOne(call, now)
		return
	}
	record, ok := c.pending[uint32(call)]
	if !ok {
		return
	}
	if messageID != c.registry.VoidID() {
		c.abandonPending(call, InvalidArgument(fmt.Sprintf(
			"end frame carried 0x%08X, which is not FRpcVoid", messageID)))
		return
	}
	if !record.shape.CalleeStreams() {
		c.abandonPending(call, InvalidArgument("END arrived on a call that carries no reply stream"))
		return
	}
	c.settlePending(call, Outcome{Kind: OutcomeEnd, Call: call})
}

func (c *Core) onCancel(messageID uint32, call CallID) {
	record, ok := c.inflight[uint32(call)]
	if !ok {
		return
	}
	safeCancel(record.handler)
	if messageID == c.registry.VoidID() {
		c.finishInflight(call, Cancelled())
		return
	}
	c.finishInflight(call, InvalidArgument(fmt.Sprintf(
		"cancel frame carried 0x%08X, which is not FRpcVoid", messageID)))
}

func (c *Core) onCredit(messageID uint32, call CallID, body []byte, now time.Time) {
	creditID, supported := c.registry.CreditID()
	if !supported {
		return
	}
	if messageID != creditID {
		status := InvalidArgument(fmt.Sprintf("credit frame carried 0x%08X, which is not FRpcCredit", messageID))
		if _, ok := c.inflight[uint32(call)]; ok {
			c.finishInflight(call, status)
		} else if _, ok := c.pending[uint32(call)]; ok {
			c.abandonPending(call, status)
		}
		return
	}
	items := readCredit(body)
	if record, ok := c.inflight[uint32(call)]; ok {
		record.lastHeard = now
		record.credit = saturatingAdd(record.credit, items)
		record.established = true
	} else if record, ok := c.pending[uint32(call)]; ok {
		record.credit = saturatingAdd(record.credit, items)
		record.established = true
	}
}

func (c *Core) rejectUndecodable(payload []byte, wireErr WireError) {
	if len(payload) < HeaderLen {
		return
	}
	c.failReceived(ReadCallID(payload), Internal(Describe(wireErr, payload)))
}

func (c *Core) expirePending(now time.Time) {
	for _, call := range sortedKeys(c.pending) {
		record := c.pending[call]
		if !record.deadline.IsZero() && !now.Before(record.deadline) {
			c.abandonPending(CallID(call), DeadlineExceeded())
		}
	}
}

func (c *Core) expireInflight(now time.Time) {
	for _, call := range sortedKeys(c.inflight) {
		record := c.inflight[call]
		expiry := record.nextExpiry()
		if !expiry.IsZero() && !now.Before(expiry) {
			safeCancel(record.handler)
			c.finishInflight(CallID(call), DeadlineExceeded())
		}
	}
}

func (c *Core) abandonPending(call CallID, status *Status) {
	requestQueued := c.outbox.RequestStillQueued(call)
	c.settlePending(call, failed(call, status))
	if !requestQueued {
		c.outbox.PushTerminal(call, c.controlFrame(call, KindCancel))
	}
}

func (c *Core) settlePending(call CallID, outcome Outcome) {
	record, ok := c.pending[uint32(call)]
	if !ok {
		return
	}
	delete(c.pending, uint32(call))
	c.outbox.Discard(call)
	c.chain.Outbound(record.entered, record.context, outcome.Status)
	c.outcomes = append(c.outcomes, outcome)
}

func (c *Core) failReceived(call CallID, status *Status) {
	if _, ok := c.inflight[uint32(call)]; ok {
		c.finishInflight(call, status)
	} else if _, ok := c.pending[uint32(call)]; ok {
		c.abandonPending(call, status)
	} else {
		c.failCall(call, status)
	}
}

func (c *Core) pollOne(call CallID, now time.Time) {
	c.collectGrant(call)
	for {
		record, ok := c.inflight[uint32(call)]
		if !ok {
			return
		}
		if !c.outbox.HasRoom(call) {
			if !c.outbox.FullFor(call) {
				return
			}
			if record.fullSince.IsZero() {
				record.fullSince = now
			}
			if now.Sub(record.fullSince) >= c.config.QueueFullLimit {
				c.finishInflight(call, ResourceExhausted("the peer stopped consuming this call"))
			}
			return
		}
		record.fullSince = time.Time{}
		if record.shape.CalleeStreams() && record.starved() {
			return
		}

		progress := safePoll(record.handler, now)
		switch progress.Kind {
		case ProgressPending:
			return

		case ProgressRespond:
			if record.shape.CalleeStreams() {
				c.finishInflight(call, Internal("streaming handler returned a unary response"))
				return
			}
			delete(c.inflight, uint32(call))
			c.outbox.PushTerminal(call, c.bodyFrame(call, KindResponse, record.replyID, progress.Body))
			c.chain.Outbound(record.entered, record.context, nil)
			return

		case ProgressItem:
			if !record.shape.CalleeStreams() {
				c.finishInflight(call, Internal("unary handler returned a stream item"))
				return
			}
			c.outbox.Push(call, c.bodyFrame(call, KindItem, record.replyID, progress.Body))
			if record.established {
				record.credit--
			}

		case ProgressEnd:
			if !record.shape.CalleeStreams() {
				c.finishInflight(call, Internal("unary handler ended without a response"))
				return
			}
			delete(c.inflight, uint32(call))
			c.outbox.PushTerminal(call, c.controlFrame(call, KindEnd))
			c.chain.Outbound(record.entered, record.context, nil)
			return

		default:
			status := progress.Status
			if status == nil {
				status = Internal("handler failed")
			}
			c.finishInflight(call, status)
			return
		}
	}
}

func (c *Core) collectGrant(call CallID) {
	creditID, supported := c.registry.CreditID()
	record, ok := c.inflight[uint32(call)]
	if !supported || !ok || !record.shape.CallerStreams() || record.inboundClosed {
		return
	}
	granter, grants := record.handler.(Granter)
	if !grants {
		return
	}
	var items uint32
	if refused := guard(func() error {
		items = granter.Grant()
		return nil
	}); refused != nil {
		c.finishInflight(call, refused)
		return
	}
	if items > 0 {
		c.outbox.Grant(call, creditID, items)
	}
}

func (c *Core) finishInflight(call CallID, status *Status) {
	record, ok := c.inflight[uint32(call)]
	delete(c.inflight, uint32(call))
	c.outbox.Discard(call)
	c.outbox.PushTerminal(call, c.bodyFrame(call, KindError, c.registry.ErrorID(), status.Encode()))
	if ok {
		c.chain.Outbound(record.entered, record.context, status)
	}
}

func (c *Core) failCall(call CallID, status *Status) {
	c.outbox.PushTerminal(call, c.bodyFrame(call, KindError, c.registry.ErrorID(), status.Encode()))
}

func (c *Core) bodyFrame(call CallID, kind Kind, messageID uint32, body []byte) Outgoing {
	sent, header := c.maybeCompress(body, Header{Kind: kind, Call: call})
	return Outgoing{MessageID: messageID, Payload: Encode(header, nil, sent)}
}

func (c *Core) controlFrame(call CallID, kind Kind) Outgoing {
	return Outgoing{MessageID: c.registry.VoidID(), Payload: Encode(Header{Kind: kind, Call: call}, nil, nil)}
}

func (c *Core) maybeCompress(body []byte, header Header) ([]byte, Header) {
	if c.compressor == nil || len(body) < c.config.CompressAbove {
		return body, header
	}
	squeezed, ok := c.compressor.Compress(body)
	if !ok || len(squeezed) >= len(body) {
		return body, header
	}
	header.Compressed = true
	return squeezed, header
}

func (c *Core) inflate(header Header, body []byte) ([]byte, *Status) {
	if !header.Compressed {
		return body, nil
	}
	if c.compressor == nil {
		return nil, Internal("a compressed body arrived but no compressor is installed")
	}
	inflated, err := c.compressor.Decompress(body, c.config.MaxBodyLen)
	switch {
	case err == nil:
		return inflated, nil
	case errors.Is(err, ErrExceedsLimit):
		return nil, ResourceExhausted("the body expands past the ceiling")
	default:
		return nil, Internal("the compressed body is malformed")
	}
}

func failed(call CallID, status *Status) Outcome {
	return Outcome{Kind: OutcomeFailed, Call: call, Status: status}
}

func guard(step func() error) (status *Status) {
	defer func() {
		switch recovered := recover().(type) {
		case nil:
		case error:
			status = StatusOf(recovered)
		default:
			status = Internal(fmt.Sprint(recovered))
		}
	}()
	if err := step(); err != nil {
		return StatusOf(err)
	}
	return nil
}

func safePoll(handler Handler, now time.Time) (progress Progress) {
	if refused := guard(func() error {
		progress = handler.Poll(now)
		return nil
	}); refused != nil {
		return Fail(refused)
	}
	return progress
}

func safeCancel(handler Handler) {
	if canceller, ok := handler.(Canceller); ok {
		guard(func() error {
			canceller.Cancel()
			return nil
		})
	}
}

func deliverItem(handler Handler, body []byte) *Status {
	receiver, ok := handler.(ItemReceiver)
	if !ok {
		return InvalidArgument("this handler accepts no caller items")
	}
	return guard(func() error { return receiver.Item(body) })
}

func closeStream(handler Handler) *Status {
	closer, ok := handler.(StreamCloser)
	if !ok {
		return nil
	}
	return guard(closer.EndOfStream)
}

func readCredit(body []byte) uint32 {
	items, _, err := NewBodyReader(body).ReadU32()
	if err != nil {
		return 0
	}
	return items
}

func earlier(current, candidate time.Time) time.Time {
	if candidate.IsZero() {
		return current
	}
	if current.IsZero() || candidate.Before(current) {
		return candidate
	}
	return current
}

func millisOf(budget time.Duration) uint32 {
	return uint32(min(max(budget.Milliseconds(), 1), math.MaxUint32))
}

func sortedKeys[V any](table map[uint32]V) []uint32 {
	return slices.Sorted(maps.Keys(table))
}
