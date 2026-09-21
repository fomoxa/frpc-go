package frpc_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	frpc "github.com/fomoxa/frpc-go"
	"github.com/fomoxa/frpc-go/examples/demo"
	"github.com/fomoxa/frpc-go/examples/demo/models"
	"github.com/fomoxa/frpc-go/rpcnet"
)

func TestALargeBodyTravelsCompressedAndArrivesIntact(t *testing.T) {
	l := newLink(t, linkSetup{})
	l.caller.SetCompressor(frpc.DeflateCompressor{})
	l.callee.SetCompressor(frpc.DeflateCompressor{})
	text := strings.Repeat("fomoxa ", 1000)
	call := l.openPlain(demo.EchoRequestID, demo.EncodeEcho(text))
	l.pump(4)
	var request, response sent
	for _, frame := range l.wire {
		switch frame.kind() {
		case frpc.KindRequest:
			request = frame
		case frpc.KindResponse:
			response = frame
		}
	}
	expectTrue(t, request.payload[0]&0x80 != 0, "bit 7 set")
	expectTrue(t, len(request.payload) < len(text)/4, "smaller on the wire")
	expectEqual(t, strings.ToUpper(text), demo.DecodeEchoResponse(l.last(call).Body), "answer")
	expectTrue(t, response.payload[0]&0x80 != 0, "the reply is compressed too")
}

func TestWithBitsSevenAndSixSetTheCtxBlockReadsWithoutDecompression(t *testing.T) {
	l := newLink(t, linkSetup{})
	l.caller.SetCompressor(frpc.DeflateCompressor{})
	l.open(demo.EchoRequestID, demo.EncodeEcho(strings.Repeat("a", 4000)), &frpc.Ctx{Tenant: "acme"}, frpc.WithoutDeadline())
	request := only(t, l.drainCaller())
	expectEqual(t, byte(0xC0), request.payload[0]&0xC0, "bits 7 and 6")
	parsed, _ := frpc.Decode(request.payload, 65536)
	ctx, _ := frpc.DecodeCtx(parsed.Ctx)
	expectEqual(t, "acme", ctx.Tenant, "readable as is")
}

func TestABodyThatExpandsPastTheCeilingIsRefusedWhileInflating(t *testing.T) {
	squeezed, _ := frpc.DeflateCompressor{}.Compress(make([]byte, 200_000))
	_, err := frpc.DeflateCompressor{}.Decompress(squeezed, 100_000)
	expectTrue(t, errors.Is(err, frpc.ErrExceedsLimit), "stopped at the limit")

	l := newLink(t, linkSetup{calleeConfig: frpc.Config{MaxBodyLen: 1000}})
	l.callee.SetCompressor(frpc.DeflateCompressor{})
	payload := frpc.Encode(frpc.Header{Kind: frpc.KindRequest, Call: 0, Compressed: true}, nil, squeezed)
	l.toCallee(demo.EchoRequestID, payload)
	expectEqual(t, frpc.CodeResourceExhausted, only(t, l.drainCallee()).code(), "code")
}

func TestACompressedBodyWithNoCompressorInstalledIsInternal(t *testing.T) {
	l := newLink(t, linkSetup{})
	payload := frpc.Encode(frpc.Header{Kind: frpc.KindRequest, Call: 0, Compressed: true}, nil, []byte{1})
	l.toCallee(demo.EchoRequestID, payload)
	expectEqual(t, frpc.CodeInternal, only(t, l.drainCallee()).code(), "code")
}

func TestAnUnsafeMethodIsNotRetriedAfterUnavailable(t *testing.T) {
	start := time.Unix(0, 0)
	decision := frpc.NewRetryState(start, 5*time.Second).OnFailure(frpc.DefaultRetryPolicy(), frpc.Unavailable(), demo.Say, start)
	expectEqual(t, frpc.GiveUpUnsafeToRepeat, decision.Reason, "unsafe")
	expectTrue(t, !decision.Retry, "no retry")
	idempotent := frpc.NewRetryState(start, 5*time.Second).
		OnFailure(frpc.DefaultRetryPolicy(), frpc.Unavailable(), demo.Say.Idempotent(), start)
	expectTrue(t, idempotent.Retry, "an idempotent method is retried")
}

func TestAKeyedMethodRepeatsItsKeyAndAnotherOperationGetsAnotherKey(t *testing.T) {
	keyed := demo.Say.Keyed()
	first := frpc.CtxForMethod(keyed, demo.EncodeEcho("order-1"))
	again := frpc.CtxForMethod(keyed, demo.EncodeEcho("order-1"))
	other := frpc.CtxForMethod(keyed, demo.EncodeEcho("order-2"))
	expectBytes(t, first.IdempotencyKey, again.IdempotencyKey, "a retry carries the same key")
	expectTrue(t, !bytes.Equal(first.IdempotencyKey, other.IdempotencyKey), "another operation, another key")
	expectEqual(t, 32, len(first.IdempotencyKey), "SHA-256 of the body")
	expectEqual(t, 0, len(frpc.CtxForMethod(demo.Say.Idempotent(), demo.EncodeEcho("x")).IdempotencyKey), "only keyed methods carry one")
}

func TestARetryDrawsFromTheFirstAttemptBudget(t *testing.T) {
	start := time.Unix(0, 0)
	policy := frpc.DefaultRetryPolicy()
	policy.MaxAttempts = 10
	policy.InitialBackoff = 400 * time.Millisecond
	state := frpc.NewRetryState(start, time.Second)
	method := demo.Say.Idempotent()
	first := state.OnFailure(policy, frpc.Unavailable(), method, start.Add(100*time.Millisecond))
	expectTrue(t, first.Retry, "the first retry fits")
	remaining, _ := state.Remaining(start.Add(600 * time.Millisecond))
	expectEqual(t, 400*time.Millisecond, remaining, "the same budget keeps running")
	second := state.OnFailure(policy, frpc.Unavailable(), method, start.Add(600*time.Millisecond))
	expectEqual(t, frpc.GiveUpBudgetExhausted, second.Reason, "the second would outlive the budget")
}

func TestStreamsAreNeverRetried(t *testing.T) {
	start := time.Unix(0, 0)
	decision := frpc.NewRetryState(start, 0).OnFailure(frpc.DefaultRetryPolicy(), frpc.Unavailable(), demo.Count.Idempotent(), start)
	expectEqual(t, frpc.GiveUpStreamingCall, decision.Reason, "streams")
}

func reflect(t *testing.T, l *link) models.FRpcReflectResponse {
	call := l.openPlain(demo.ReflectRequestID, demo.EncodeReflectRequest())
	l.pump(4)
	return demo.DecodeReflectResponse(l.last(call).Body)
}

func TestTheReflectionAnswerListsEveryServedMethodWithShapeAndIDs(t *testing.T) {
	answer := reflect(t, newLink(t, linkSetup{}))
	var names []string
	byName := map[string]models.FRpcMethodInfo{}
	for _, info := range answer.Methods {
		names = append(names, info.Name)
		byName[info.Name] = info
	}
	expectSequence(t, []string{"Echo.Say", "Echo.Count", "Blob.Upload", "Room.Chat", "Clock.Ticks", "FRpc.Reflect"}, names, "names")
	upload := byName["Blob.Upload"]
	expectEqual(t, uint32(2), upload.Shape, "client stream")
	expectEqual(t, demo.UploadChunkID, upload.CallerItemId, "caller item id")
	say := byName["Echo.Say"]
	expectEqual(t, say.RequestId, say.CallerItemId, "no caller item: equals the request id")
	expectEqual(t, uint32(1), byName["FRpc.Reflect"].RetrySafety, "reflection is idempotent")
}

func TestSchemaJSONIsTheEmbeddedFileByteForByte(t *testing.T) {
	expectBytes(t, demo.SchemaJSON, reflect(t, newLink(t, linkSetup{})).SchemaJson, "schema_json")
}

func TestReflectionRefusesAnUnauthenticatedSessionUnlessTheOperatorExemptsIt(t *testing.T) {
	l := newLink(t, linkSetup{})
	l.callee.AddInterceptor(frpc.NewAuthorization())
	refused := l.openPlain(demo.ReflectRequestID, demo.EncodeReflectRequest())
	l.pump(4)
	expectFailed(t, l.last(refused), frpc.CodeUnauthenticated, "not exempt by default")

	exempted := newLink(t, linkSetup{})
	exempted.callee.AddInterceptor(frpc.NewAuthorization().Exempting(demo.ReflectRequestID))
	served := exempted.openPlain(demo.ReflectRequestID, demo.EncodeReflectRequest())
	exempted.pump(4)
	expectEqual(t, frpc.OutcomeResponse, exempted.last(served).Kind, "the operator's exemption")
}

func TestAServerWithoutReflectionAnswersUnimplementedWhetherOrNotItDeclaresTheModels(t *testing.T) {
	l := newLink(t, linkSetup{callee: demo.ResponderRegistry(false)})
	call := l.openPlain(demo.ReflectRequestID, demo.EncodeReflectRequest())
	request := only(t, l.drainCaller())
	expectTrue(t, l.toCallee(request.messageID, request.payload), "never handed to the application")
	for _, frame := range l.drainCallee() {
		l.toCaller(frame.messageID, frame.payload)
	}
	expectFailed(t, l.last(call), frpc.CodeUnimplemented, "reflection off")
}

func expectRegistryError(t *testing.T, err error, fragment string) {
	t.Helper()
	var registryErr *frpc.RegistryError
	if !errors.As(err, &registryErr) {
		t.Fatalf("the registry was accepted: %v", err)
	}
	expectTrue(t, strings.Contains(registryErr.Reason, fragment), "message mentions "+fragment+": "+registryErr.Reason)
}

func TestTwoMethodsSharingARequestTypeFailAtStartup(t *testing.T) {
	_, err := demo.Builder().Declare(demo.Say).Declare(slow).Build()
	expectRegistryError(t, err, "share request type")
}

func TestAnFRpcIDCollidingWithAPlainMessageIDFailsAtStartup(t *testing.T) {
	_, err := demo.Builder().PlainMessage(demo.EchoResponseID).Declare(demo.Say).Build()
	expectRegistryError(t, err, "plain message")
}

func TestAMethodUsingASystemModelIDFailsAtStartup(t *testing.T) {
	_, err := demo.Builder().Declare(frpc.Unary("Bad", demo.EchoRequestID, demo.VoidID)).Build()
	expectRegistryError(t, err, "system model")
}

func TestADescriptorTypeAbsentFromTheSchemaFailsAtStartup(t *testing.T) {
	registry := build(t, demo.Builder().Declare(frpc.Unary("Ghost", 0x0DDBA11, demo.EchoResponseID)))
	expectRegistryError(t, rpcnet.RequireDeclared(registry, demo.NetSchema()), "does not declare")
}
