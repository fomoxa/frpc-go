package frpc_test

import (
	"testing"
	"time"

	frpc "github.com/fomoxa/frpc-go"
	"github.com/fomoxa/frpc-go/examples/demo"
)

func guarded(t *testing.T, authorization *frpc.Authorization, say frpc.Method) *link {
	callee := build(t, demo.Builder().
		Serve(say, func(_ frpc.Method, body []byte) (frpc.Handler, error) {
			return frpc.RespondNow(demo.EncodeEchoResponse(demo.DecodeEcho(body))), nil
		}).
		Serve(demo.Count, func(frpc.Method, []byte) (frpc.Handler, error) { return frpc.ItemsOf(nil), nil }))
	l := newLink(t, linkSetup{callee: callee})
	l.callee.AddInterceptor(authorization)
	return l
}

func TestAnUnauthenticatedSessionCallingANonExemptMethodIsUnauthenticated(t *testing.T) {
	l := guarded(t, frpc.NewAuthorization(), demo.Say)
	call := l.openPlain(demo.EchoRequestID, demo.EncodeEcho("x"))
	l.pump(4)
	expectFailed(t, l.last(call), frpc.CodeUnauthenticated, "no identity")
}

func TestAnExemptMethodRunsBeforeAuthentication(t *testing.T) {
	l := guarded(t, frpc.NewAuthorization().Exempting(demo.EchoRequestID), demo.Say)
	exempt := l.openPlain(demo.EchoRequestID, demo.EncodeEcho("x"))
	other := l.openPlain(demo.CountRequestID, demo.EncodeCount(1))
	l.pump(4)
	expectEqual(t, frpc.OutcomeResponse, l.last(exempt).Kind, "exempt method")
	expectFailed(t, l.last(other), frpc.CodeUnauthenticated, "the other one")
}

func TestAPerCallTokenOfAnotherPrincipalIsPermissionDeniedAndChangesNothing(t *testing.T) {
	ran := false
	callee := build(t, demo.Builder().Serve(demo.Say, func(frpc.Method, []byte) (frpc.Handler, error) {
		ran = true
		return frpc.RespondNow(demo.EncodeEchoResponse("x")), nil
	}))
	l := newLink(t, linkSetup{callee: callee})
	l.callee.AddInterceptor(frpc.NewAuthorization().Verifying(func(token string) *frpc.Identity {
		return frpc.NewIdentity(token)
	}))
	l.callee.Authenticate(frpc.NewIdentity("alice"))
	call := l.open(demo.EchoRequestID, demo.EncodeEcho("x"), &frpc.Ctx{Token: "mallory"}, frpc.WithoutDeadline())
	l.pump(4)
	expectFailed(t, l.last(call), frpc.CodePermissionDenied, "another principal")
	expectTrue(t, !ran, "the handler did not run")
	expectEqual(t, "alice", l.callee.Identity().Principal, "identity unchanged")
}

func TestDelegationNarrowsToTheIntersectionOfBothPermissionSets(t *testing.T) {
	var effective []string
	l := guarded(t, frpc.NewAuthorization().
		Verifying(func(token string) *frpc.Identity { return frpc.NewIdentity(token, "read", "export") }).
		AllowingDelegation(), demo.Say)
	l.callee.AddInterceptor(&probe{seen: func(context *frpc.CallContext) { effective = context.Permissions }})
	l.callee.Authenticate(frpc.NewIdentity("service", "read", "write"))
	call := l.open(demo.EchoRequestID, demo.EncodeEcho("x"), &frpc.Ctx{Token: "user"}, frpc.WithoutDeadline())
	l.pump(4)
	expectEqual(t, frpc.OutcomeResponse, l.last(call).Kind, "served")
	expectSequence(t, []string{"read"}, effective, "intersection, never the union")
}

func TestAMissingPermissionIsPermissionDenied(t *testing.T) {
	l := guarded(t, frpc.NewAuthorization(), demo.Say.Requiring("echo"))
	l.callee.Authenticate(frpc.NewIdentity("bob", "other"))
	call := l.openPlain(demo.EchoRequestID, demo.EncodeEcho("x"))
	l.pump(4)
	expectFailed(t, l.last(call), frpc.CodePermissionDenied, "lacks echo")
}

func TestAnInterceptorServingFromAColdCacheAnswersUnavailableAndTheLoopGoesOn(t *testing.T) {
	l := guarded(t, frpc.NewAuthorization().Exempting(demo.EchoRequestID), demo.Say)
	l.callee.AddInterceptor(&coldCache{})
	call := l.openPlain(demo.EchoRequestID, demo.EncodeEcho("x"))
	l.pump(4)
	expectFailed(t, l.last(call), frpc.CodeUnavailable, "answered at once")
	after := l.openPlain(demo.CountRequestID, demo.EncodeCount(0))
	l.pump(4)
	expectEqual(t, 2, len(l.outcomes), "the loop keeps serving")
	expectTrue(t, l.last(after).Terminates(), "a later call completes")
}

func TestAuthenticationRunsBeforeDecompression(t *testing.T) {
	trap := &trap{}
	l := guarded(t, frpc.NewAuthorization(), demo.Say)
	l.callee.SetCompressor(trap)
	payload := frpc.Encode(frpc.Header{Kind: frpc.KindRequest, Call: 0, Compressed: true, HasCtx: true},
		frpc.Ctx{Token: "t"}.Encode(), []byte{1, 2, 3})
	l.toCallee(demo.EchoRequestID, payload)
	expectEqual(t, frpc.CodeUnauthenticated, only(t, l.drainCallee()).code(), "rejected on the ctx alone")
	expectTrue(t, !trap.touched, "the body was never decompressed")
}

func TestTheCallerChainCompletesBeforeTheCtxIsEncoded(t *testing.T) {
	l := newLink(t, linkSetup{})
	l.caller.AddInterceptor(attach{})
	l.openPlain(demo.EchoRequestID, demo.EncodeEcho("x"))
	request := only(t, l.drainCaller())
	parsed, _ := frpc.Decode(request.payload, 65536)
	ctx, _ := frpc.DecodeCtx(parsed.Ctx)
	expectEqual(t, "attached", ctx.Token, "the token set by the chain is on the wire")
}

func TestTheChainUnwindsExactlyOnceOnEveryCompletionPath(t *testing.T) {
	counter := &counting{}
	l := newLink(t, linkSetup{caller: slowCaller(t), callee: slowCallee(t, nil)})
	l.caller.AddInterceptor(counter)
	answered := newLink(t, linkSetup{})
	answered.caller.AddInterceptor(counter)
	served := answered.openPlain(demo.EchoRequestID, demo.EncodeEcho("x"))
	answered.pump(4)
	l.open(slow.RequestID, demo.EncodeEcho("x"), nil, frpc.Within(10*time.Millisecond))
	l.openPlain(slow.RequestID, demo.EncodeEcho("y"))
	l.pump(4)
	l.now = l.now.Add(time.Second)
	l.pump(4)
	l.caller.OnSessionEnd()
	expectEqual(t, frpc.OutcomeResponse, answered.last(served).Kind, "one answered")
	expectEqual(t, 3, counter.entered, "three calls entered")
	expectEqual(t, 3, counter.unwound, "each unwound once: response, deadline, session end")
}

func TestACallRefusedLocallyAfterTheCallerChainRanStillUnwindsIt(t *testing.T) {
	counter := &counting{}
	l := newLink(t, linkSetup{callerConfig: frpc.Config{MaxConnectionQueue: 1, MaxCtxLen: 16}})
	l.caller.AddInterceptor(counter)
	_, err := l.caller.Call(demo.EchoRequestID, demo.EncodeEcho("x"), &frpc.Ctx{Tenant: "a tenant name longer than sixteen bytes"}, frpc.WithoutDeadline(), l.now)
	expectTrue(t, err != nil, "the ctx is over its ceiling")
	l.openPlain(demo.EchoRequestID, demo.EncodeEcho("fills the connection"))
	_, err = l.caller.Call(demo.EchoRequestID, demo.EncodeEcho("y"), nil, frpc.WithoutDeadline(), l.now)
	expectTrue(t, err != nil, "the connection is full")
	expectEqual(t, 3, counter.entered, "three calls entered the chain")
	expectEqual(t, 2, counter.unwound, "both refused calls unwound, the queued one has not completed")
}

type coldCache struct {
	frpc.PassThrough
}

func (coldCache) Inbound(*frpc.CallContext) *frpc.Status {
	return frpc.NewStatus(frpc.CodeUnavailable, "keys not loaded")
}

type attach struct {
	frpc.PassThrough
}

func (attach) Outgoing(context *frpc.CallContext) *frpc.Status {
	context.Ctx.Token = "attached"
	return nil
}

type counting struct {
	frpc.PassThrough
	entered int
	unwound int
}

func (c *counting) Outgoing(*frpc.CallContext) *frpc.Status {
	c.entered++
	return nil
}

func (c *counting) Outbound(*frpc.CallContext, *frpc.Status) { c.unwound++ }

type trap struct {
	touched bool
}

func (t *trap) Compress([]byte) ([]byte, bool) { return nil, false }

func (t *trap) Decompress([]byte, int) ([]byte, error) {
	t.touched = true
	return nil, frpc.ErrMalformedBody
}
