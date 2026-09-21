//go:build unix

package rpcnet

import (
	"errors"
	"time"

	fomoxa "github.com/fomoxa/go"

	frpc "github.com/fomoxa/frpc-go"
)

var ErrNotPlain = errors.New("rpcnet: the message id is not a declared plain message")

type PlainMessage struct {
	MessageID uint32
	Payload   []byte
}

type Wait struct {
	Timeout  time.Duration
	Writable bool
}

type WaitPolicy struct {
	IdleCap  time.Duration
	WorkPoll time.Duration
}

func DefaultWaitPolicy() WaitPolicy {
	return WaitPolicy{IdleCap: 250 * time.Millisecond, WorkPoll: 200 * time.Microsecond}
}

func (p WaitPolicy) normalized() WaitPolicy {
	defaults := DefaultWaitPolicy()
	if p.IdleCap <= 0 {
		p.IdleCap = defaults.IdleCap
	}
	if p.WorkPoll <= 0 {
		p.WorkPoll = defaults.WorkPoll
	}
	return p
}

func (p WaitPolicy) Plan(core *frpc.Core, netCongested bool, now time.Time) Wait {
	p = p.normalized()
	timeout := p.IdleCap
	if core.HasRunnableWork() {
		timeout = min(timeout, p.WorkPoll)
	}
	if deadline, ok := core.NextDeadline(); ok {
		timeout = min(timeout, max(deadline.Sub(now), 0))
	}
	return Wait{Timeout: timeout, Writable: netCongested || core.HasUnsentFrames()}
}

func RequireDeclared(registry *frpc.Registry, schema *fomoxa.Schema) error {
	declared := make(map[uint32]struct{}, len(schema.Messages()))
	for _, message := range schema.Messages() {
		declared[message.ID] = struct{}{}
	}
	return registry.RequireDeclaredIn(func(id uint32) bool {
		_, ok := declared[id]
		return ok
	})
}

type Session struct {
	transport *SocketTransport
	conn      *fomoxa.Conn
	core      *frpc.Core
	plain     []PlainMessage
	refusal   *fomoxa.Verdict
}

func NewSession(
	transport *SocketTransport,
	schema *fomoxa.Schema,
	registry *frpc.Registry,
	role frpc.Role,
	netConfig fomoxa.Config,
	config frpc.Config,
) (*Session, error) {
	if err := RequireDeclared(registry, schema); err != nil {
		return nil, err
	}
	var conn *fomoxa.Conn
	var err error
	if role == frpc.RoleClient {
		conn, err = fomoxa.NewConn(transport, schema, netConfig)
	} else {
		conn, err = fomoxa.NewPeerConn(transport, schema, netConfig)
	}
	if err != nil {
		return nil, err
	}
	return &Session{transport: transport, conn: conn, core: frpc.NewCore(registry, role, config)}, nil
}

func (s *Session) Core() *frpc.Core { return s.core }

func (s *Session) Transport() *SocketTransport { return s.transport }

func (s *Session) Ready() bool { return s.core.Ready() }

func (s *Session) Closed() bool { return s.conn.State() == fomoxa.StateClosed }

func (s *Session) State() fomoxa.State { return s.conn.State() }

func (s *Session) Refusal() (fomoxa.Verdict, bool) {
	if s.refusal == nil {
		return fomoxa.VerdictAccept, false
	}
	return *s.refusal, true
}

func (s *Session) Call(requestID uint32, body []byte, ctx *frpc.Ctx, options frpc.CallOptions, now time.Time) (frpc.CallID, error) {
	return s.core.Call(requestID, body, ctx, options, now)
}

func (s *Session) Tick(now time.Time) {
	for _, event := range s.conn.Tick(now) {
		switch event.Kind {
		case fomoxa.EventReady:
			s.core.OnReady()
		case fomoxa.EventMessage:
			if !s.core.OnMessage(event.MessageID, event.Payload, now) {
				s.plain = append(s.plain, PlainMessage{MessageID: event.MessageID, Payload: event.Payload})
			}
		case fomoxa.EventHandshakeFailed:
			verdict := event.Verdict
			s.refusal = &verdict
			s.core.OnSessionEnd()
		case fomoxa.EventDisconnected:
			s.core.OnSessionEnd()
		}
	}
	s.core.Tick(now)
	s.core.Drain(func(frame frpc.Outgoing) frpc.Delivery {
		return deliveryOf(s.conn.Send(frame.MessageID, frame.Payload))
	})
}

func (s *Session) TakeOutcomes() []frpc.Outcome { return s.core.TakeOutcomes() }

func (s *Session) TakePlain() []PlainMessage {
	taken := s.plain
	s.plain = nil
	return taken
}

func (s *Session) SendPlain(messageID uint32, payload []byte) error {
	if !s.core.Registry().IsPlain(messageID) {
		return ErrNotPlain
	}
	return s.conn.Send(messageID, payload)
}

func (s *Session) Wait(policy WaitPolicy, now time.Time) (bool, error) {
	plan := policy.Plan(s.core, s.transport.Backlogged(), now)
	return s.transport.WaitReady(plan.Writable, plan.Timeout)
}

func (s *Session) Close() {
	_ = s.conn.Close()
	s.core.OnSessionEnd()
}

func deliveryOf(err error) frpc.Delivery {
	switch {
	case err == nil:
		return frpc.Accepted
	case errors.Is(err, fomoxa.ErrCongested), errors.Is(err, fomoxa.ErrNotReady), errors.Is(err, fomoxa.ErrSessionClosed):
		return frpc.Rejected
	default:
		return frpc.Accepted
	}
}
