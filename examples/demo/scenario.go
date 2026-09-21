//go:build unix

package demo

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	frpc "github.com/fomoxa/frpc-go"
	"github.com/fomoxa/frpc-go/examples/demo/generated"
	"github.com/fomoxa/frpc-go/rpcnet"
)

var Missing = frpc.Unary("Echo.Missing", CtxID, EchoResponseID)

func DriverRegistry() *frpc.Registry {
	return must(Builder().
		Declare(Reflect).
		Declare(Say).
		Declare(Count).
		Declare(Upload).
		Declare(Chat).
		Declare(Ticks).
		Declare(Missing).
		Build())
}

type check struct {
	name string
	body func(*rpcnet.Client) error
}

func Drive(client *rpcnet.Client, log io.Writer) int {
	checks := []check{
		{"unary Echo.Say", sayUppercases},
		{"unary error Echo.Say empty", sayRejectsEmpty},
		{"server stream Echo.Count", countStreams},
		{"server stream error Echo.Count 101", countRejectsLarge},
		{"client stream Blob.Upload", uploadSummarises},
		{"client stream error Blob.Upload empty chunk", uploadRejectsEmptyChunk},
		{"bidi Room.Chat", chatEchoes},
		{"credit Clock.Ticks", ticksFollowCredit},
		{"deadline Clock.Ticks", ticksExpire},
		{"unimplemented Echo.Missing", missingIsUnimplemented},
		{"reflection FRpc.Reflect", reflectionLists},
	}
	failures := 0
	for _, each := range checks {
		if problem := run(each.body, client); problem != nil {
			failures++
			fmt.Fprintf(log, "FAIL %s: %v\n", each.name, problem)
		} else {
			fmt.Fprintf(log, "ok   %s\n", each.name)
		}
	}
	return failures
}

func run(body func(*rpcnet.Client) error, client *rpcnet.Client) (problem error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			problem = fmt.Errorf("%v", recovered)
		}
	}()
	return body(client)
}

func Frames() []string {
	now := time.Unix(0, 0)
	client := frpc.NewCore(DriverRegistry(), frpc.RoleClient, frpc.Config{})
	server := frpc.NewCore(ResponderRegistry(false), frpc.RoleServer, frpc.Config{})
	client.OnReady()
	server.OnReady()
	var lines []string

	traced := &frpc.Ctx{
		DeadlineMs: 1500,
		TraceID:    []byte{1, 2, 3, 4},
		SpanID:     []byte{5, 6},
		Token:      "tok",
		Tenant:     "acme",
	}
	open := func(requestID uint32, body []byte, ctx *frpc.Ctx) frpc.CallID {
		call, err := client.Call(requestID, body, ctx, frpc.WithoutDeadline(), now)
		if err != nil {
			panic(err)
		}
		return call
	}
	open(EchoRequestID, EncodeEcho("xin chao"), traced)
	ticks := open(TicksRequestID, EncodeTicksRequest(), frpc.WithInitialCredit(2))
	_ = client.Grant(ticks, 3)
	upload := open(UploadOpenID, EncodeUploadOpen("photo.raw"), nil)
	_ = client.SendItem(upload, EncodeChunk(4096))
	_ = client.SendItem(upload, EncodeChunk(1024))
	_ = client.CloseSend(upload)
	open(EchoRequestID, EncodeEcho(""), nil)
	open(Missing.RequestID, nil, nil)
	chat := open(ChatOpenID, EncodeRoom("lobby"), nil)
	_ = client.SendItem(chat, EncodeSaid("hello"))
	_ = client.CloseSend(chat)

	exchange(client, server, now, &lines)
	client.Cancel(ticks)
	exchange(client, server, now, &lines)
	return lines
}

func exchange(client, server *frpc.Core, now time.Time, lines *[]string) {
	for _, frame := range take(client, "C", lines) {
		server.OnMessage(frame.MessageID, frame.Payload, now)
	}
	server.Tick(now)
	for _, frame := range take(server, "S", lines) {
		client.OnMessage(frame.MessageID, frame.Payload, now)
	}
	client.Tick(now)
	client.TakeOutcomes()
}

func take(core *frpc.Core, side string, lines *[]string) []frpc.Outgoing {
	var frames []frpc.Outgoing
	core.Drain(func(frame frpc.Outgoing) frpc.Delivery {
		frames = append(frames, frame)
		*lines = append(*lines, fmt.Sprintf("%s %08X %X", side, frame.MessageID, frame.Payload))
		return frpc.Accepted
	})
	return frames
}

func open(client *rpcnet.Client, requestID uint32, body []byte, ctx *frpc.Ctx) frpc.CallID {
	call, err := client.Call(requestID, body, ctx, frpc.DefaultCallOptions())
	if err != nil {
		panic(fmt.Sprintf("the call could not be sent: %v", err))
	}
	return call
}

func next(client *rpcnet.Client, call frpc.CallID) frpc.Outcome {
	outcome, err := client.Next(call)
	if err != nil {
		panic(err.Error())
	}
	return outcome
}

func nextTick(client *rpcnet.Client, call frpc.CallID) uint32 {
	outcome := next(client, call)
	if outcome.Kind != frpc.OutcomeItem {
		panic(fmt.Sprintf("expected a tick, got %s", outcome))
	}
	return DecodeTick(outcome.Body)
}

func expectStatus(outcome frpc.Outcome, code uint32) error {
	if outcome.Kind == frpc.OutcomeFailed && outcome.Status.Code == code {
		return nil
	}
	return fmt.Errorf("expected %s, got %s", frpc.CodeName(code), outcome)
}

func invoke(client *rpcnet.Client, requestID uint32, body []byte) frpc.Outcome {
	outcome, err := client.Invoke(requestID, body)
	if err != nil {
		panic(err.Error())
	}
	return outcome
}

func sayUppercases(client *rpcnet.Client) error {
	outcome := invoke(client, EchoRequestID, EncodeEcho("xin chao"))
	if outcome.Kind != frpc.OutcomeResponse {
		return errors.New(outcome.String())
	}
	if text := DecodeEchoResponse(outcome.Body); text != "XIN CHAO" {
		return fmt.Errorf("answered %s", text)
	}
	return nil
}

func sayRejectsEmpty(client *rpcnet.Client) error {
	return expectStatus(invoke(client, EchoRequestID, EncodeEcho("")), frpc.CodeInvalidArgument)
}

func countStreams(client *rpcnet.Client) error {
	outcomes, err := client.Collect(open(client, CountRequestID, EncodeCount(5), nil))
	if err != nil {
		return err
	}
	var seen []uint32
	for _, outcome := range outcomes {
		switch outcome.Kind {
		case frpc.OutcomeItem:
			seen = append(seen, DecodeCountItem(outcome.Body))
		case frpc.OutcomeEnd:
			if slices.Equal(seen, []uint32{1, 2, 3, 4, 5}) {
				return nil
			}
			return fmt.Errorf("items %v", seen)
		default:
			return errors.New(outcome.String())
		}
	}
	return errors.New("the stream ended without END")
}

func countRejectsLarge(client *rpcnet.Client) error {
	return expectStatus(next(client, open(client, CountRequestID, EncodeCount(101), nil)), frpc.CodeInvalidArgument)
}

func uploadSummarises(client *rpcnet.Client) error {
	call := open(client, UploadOpenID, EncodeUploadOpen("photo.raw"), nil)
	for _, size := range []uint32{4096, 8192, 1024} {
		if err := client.SendItem(call, EncodeChunk(size)); err != nil {
			return fmt.Errorf("sending a chunk: %w", err)
		}
	}
	_ = client.CloseSend(call)
	outcome := next(client, call)
	if outcome.Kind != frpc.OutcomeResponse {
		return errors.New(outcome.String())
	}
	if summary := DecodeDone(outcome.Body); summary != "3 chunks, 13312 bytes" {
		return fmt.Errorf("answered %s", summary)
	}
	return nil
}

func uploadRejectsEmptyChunk(client *rpcnet.Client) error {
	call := open(client, UploadOpenID, EncodeUploadOpen("empty.raw"), nil)
	_ = client.SendItem(call, EncodeChunk(0))
	return expectStatus(next(client, call), frpc.CodeInvalidArgument)
}

func chatEchoes(client *rpcnet.Client) error {
	call := open(client, ChatOpenID, EncodeRoom("lobby"), nil)
	for _, line := range []string{"hello", "still here", "bye"} {
		_ = client.SendItem(call, EncodeSaid(line))
		heard := next(client, call)
		if heard.Kind != frpc.OutcomeItem {
			return errors.New(heard.String())
		}
		if text := DecodeHeard(heard.Body); text != strings.ToUpper(line) {
			return fmt.Errorf("heard %s for %s", text, line)
		}
	}
	_ = client.CloseSend(call)
	if last := next(client, call); last.Kind != frpc.OutcomeEnd {
		return errors.New(last.String())
	}
	return nil
}

func ticksFollowCredit(client *rpcnet.Client) error {
	call := open(client, TicksRequestID, EncodeTicksRequest(), frpc.WithInitialCredit(2))
	seen := []uint32{nextTick(client, call), nextTick(client, call)}
	quietUntil := time.Now().Add(150 * time.Millisecond)
	for time.Now().Before(quietUntil) {
		_, _ = client.Session().Wait(rpcnet.DefaultWaitPolicy(), time.Now())
		client.Pump()
	}
	if _, pending := client.Core().PendingRequestID(call); !pending {
		return errors.New("the stream ended while the allowance was zero")
	}
	_ = client.Grant(call, 3)
	seen = append(seen, nextTick(client, call), nextTick(client, call), nextTick(client, call))
	client.Cancel(call)
	if cancelled := next(client, call); cancelled.Status == nil || cancelled.Status.Code != frpc.CodeCancelled {
		return fmt.Errorf("after cancel: %s", cancelled)
	}
	if !slices.Equal(seen, []uint32{1, 2, 3, 4, 5}) {
		return fmt.Errorf("items %v", seen)
	}
	return nil
}

func ticksExpire(client *rpcnet.Client) error {
	call, err := client.Call(TicksRequestID, EncodeTicksRequest(), frpc.WithInitialCredit(1), frpc.Within(200*time.Millisecond))
	if err != nil {
		return err
	}
	nextTick(client, call)
	return expectStatus(next(client, call), frpc.CodeDeadlineExceeded)
}

func missingIsUnimplemented(client *rpcnet.Client) error {
	return expectStatus(invoke(client, Missing.RequestID, nil), frpc.CodeUnimplemented)
}

func reflectionLists(client *rpcnet.Client) error {
	outcome := invoke(client, ReflectRequestID, EncodeReflectRequest())
	if outcome.Kind != frpc.OutcomeResponse {
		return errors.New(outcome.String())
	}
	answer := DecodeReflectResponse(outcome.Body)
	var expected, listed []string
	for _, method := range append(Methods(), Reflect.Idempotent()) {
		expected = append(expected, fmt.Sprintf("%s/%d/%08X/%08X/%08X/%d",
			method.Name, method.Shape, method.RequestID, method.CallerItemID(), method.ReplyID, method.RetrySafety))
	}
	for _, info := range answer.Methods {
		listed = append(listed, fmt.Sprintf("%s/%d/%08X/%08X/%08X/%d",
			info.Name, info.Shape, info.RequestId, info.CallerItemId, info.ReplyId, info.RetrySafety))
	}
	slices.Sort(expected)
	slices.Sort(listed)
	if !slices.Equal(expected, listed) {
		return fmt.Errorf("listed %s", strings.Join(listed, " "))
	}
	var schema struct {
		Fingerprint string `json:"fingerprint_u64"`
	}
	if err := json.Unmarshal(answer.SchemaJson, &schema); err != nil {
		return err
	}
	if want := fmt.Sprintf("0x%016X", generated.FomoxaSchemaFingerprint); schema.Fingerprint != want {
		return fmt.Errorf("schema fingerprint %s", schema.Fingerprint)
	}
	return nil
}
