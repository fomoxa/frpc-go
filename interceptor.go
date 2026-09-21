package frpc

import (
	"fmt"
	"math"
	"slices"
	"time"
)

type CallContext struct {
	Call        CallID
	Method      Method
	Now         time.Time
	Deadline    time.Time
	Ctx         Ctx
	Session     *Identity
	OnBehalfOf  *Identity
	Permissions []string
	Side        Sender
}

func newCallContext(call CallID, method Method, now time.Time, ctx Ctx, session *Identity, side Sender) *CallContext {
	context := &CallContext{Call: call, Method: method, Now: now, Ctx: ctx, Session: session, Side: side}
	if session != nil {
		context.Permissions = slices.Clone(session.Permissions)
	}
	return context
}

func (c *CallContext) HasDeadline() bool { return !c.Deadline.IsZero() }

func (c *CallContext) Permits(permission string) bool {
	_, found := slices.BinarySearch(c.Permissions, permission)
	return found
}

func (c *CallContext) RemainingMs(now time.Time) (uint32, bool) {
	if !c.HasDeadline() {
		return 0, false
	}
	left := c.Deadline.Sub(now)
	if left <= 0 {
		return 0, true
	}
	return uint32(min(left.Milliseconds(), math.MaxUint32)), true
}

func (c *CallContext) OnwardCtx(now time.Time) (*Ctx, bool) {
	remaining, bounded := c.RemainingMs(now)
	if bounded && remaining == 0 {
		return nil, false
	}
	return &Ctx{
		DeadlineMs: remaining,
		TraceID:    c.Ctx.TraceID,
		SpanID:     c.Ctx.SpanID,
		Tenant:     c.Ctx.Tenant,
	}, true
}

type Interceptor interface {
	Outgoing(context *CallContext) *Status
	Inbound(context *CallContext) *Status
	Outbound(context *CallContext, status *Status)
}

type PassThrough struct{}

func (PassThrough) Outgoing(*CallContext) *Status { return nil }

func (PassThrough) Inbound(*CallContext) *Status { return nil }

func (PassThrough) Outbound(*CallContext, *Status) {}

type Chain struct {
	links []Interceptor
}

func (c *Chain) Add(link Interceptor) { c.links = append(c.links, link) }

func (c *Chain) Len() int { return len(c.links) }

func (c *Chain) Outgoing(context *CallContext) (int, *Status) {
	for index, link := range c.links {
		if rejection := link.Outgoing(context); rejection != nil {
			return index, rejection
		}
	}
	return len(c.links), nil
}

func (c *Chain) Inbound(context *CallContext) (int, *Status) {
	for index, link := range c.links {
		if rejection := link.Inbound(context); rejection != nil {
			return index, rejection
		}
	}
	return len(c.links), nil
}

func (c *Chain) Outbound(entered int, context *CallContext, status *Status) {
	for index := min(entered, len(c.links)) - 1; index >= 0; index-- {
		c.links[index].Outbound(context, status)
	}
}

type Authorization struct {
	PassThrough
	exempt          map[uint32]struct{}
	verifier        TokenVerifier
	allowDelegation bool
}

func NewAuthorization() *Authorization {
	return &Authorization{exempt: map[uint32]struct{}{}}
}

func (a *Authorization) Exempting(requestID uint32) *Authorization {
	a.exempt[requestID] = struct{}{}
	return a
}

func (a *Authorization) Verifying(verifier TokenVerifier) *Authorization {
	a.verifier = verifier
	return a
}

func (a *Authorization) AllowingDelegation() *Authorization {
	a.allowDelegation = true
	return a
}

func (a *Authorization) Inbound(context *CallContext) *Status {
	session := context.Session
	if session == nil {
		if _, exempt := a.exempt[context.Method.RequestID]; exempt {
			return nil
		}
		return Unauthenticated("the session has no identity")
	}

	context.Permissions = slices.Clone(session.Permissions)

	if context.Ctx.Token != "" {
		if a.verifier == nil {
			return Unauthenticated("a per-call credential was presented but no verifier is installed")
		}
		presented := a.verifier(context.Ctx.Token)
		if presented == nil {
			return Unauthenticated("the per-call credential did not verify")
		}
		if presented.Principal != session.Principal {
			if !a.allowDelegation {
				return PermissionDenied("a per-call credential may not replace the session identity")
			}
			context.Permissions = session.NarrowedBy(presented)
			context.OnBehalfOf = presented
		}
	}

	if required := context.Method.Permission; required != "" && !context.Permits(required) {
		return PermissionDenied(fmt.Sprintf("%s requires %s", context.Method.Name, required))
	}
	return nil
}
