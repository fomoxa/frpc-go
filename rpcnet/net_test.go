//go:build unix

package rpcnet_test

import (
	"bytes"
	"errors"
	"net"
	"strings"
	"syscall"
	"testing"
	"time"

	fomoxa "github.com/fomoxa/go"

	frpc "github.com/fomoxa/frpc-go"
	"github.com/fomoxa/frpc-go/examples/demo"
	"github.com/fomoxa/frpc-go/rpcnet"
)

func startServer(t *testing.T, registry *frpc.Registry, schema *fomoxa.Schema, observer rpcnet.ServerObserver) *rpcnet.Server {
	t.Helper()
	if registry == nil {
		registry = demo.ResponderRegistry(true)
	}
	if schema == nil {
		schema = demo.NetSchema()
	}
	server, err := rpcnet.Bind("127.0.0.1:0", schema, registry, fomoxa.Config{}, frpc.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if observer != nil {
		server.Observing(observer)
	}
	server.Start()
	t.Cleanup(server.Stop)
	return server
}

func connect(t *testing.T, server *rpcnet.Server, registry *frpc.Registry, schema *fomoxa.Schema) (*rpcnet.Client, error) {
	t.Helper()
	if registry == nil {
		registry = demo.DriverRegistry()
	}
	if schema == nil {
		schema = demo.NetSchema()
	}
	client, err := rpcnet.Connect(server.Addr().String(), schema, registry, 5*time.Second, rpcnet.ClientConfig{})
	if err == nil {
		t.Cleanup(client.Close)
	}
	return client, err
}

func mustConnect(t *testing.T, server *rpcnet.Server, registry *frpc.Registry) *rpcnet.Client {
	t.Helper()
	client, err := connect(t, server, registry, nil)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func build(t *testing.T, builder *frpc.RegistryBuilder) *frpc.Registry {
	t.Helper()
	registry, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func TestTheFullScenarioPassesAgainstTheServer(t *testing.T) {
	server := startServer(t, nil, nil, nil)
	client := mustConnect(t, server, nil)
	var log strings.Builder
	if failures := demo.Drive(client, &log); failures != 0 {
		t.Fatal(log.String())
	}
}

type collector struct {
	rpcnet.NopObserver
	seen chan rpcnet.PlainMessage
}

func (c collector) Plain(_ uint64, message rpcnet.PlainMessage) { c.seen <- message }

func TestADeclaredPlainMessageReachesTheApplicationBesideRPCTraffic(t *testing.T) {
	plainRegistry := func() *frpc.Registry {
		return build(t, demo.Builder().PlainMessage(demo.TicksItemID).Serve(demo.Say,
			func(_ frpc.Method, body []byte) (frpc.Handler, error) {
				return frpc.RespondNow(demo.EncodeEchoResponse(demo.DecodeEcho(body))), nil
			}))
	}
	seen := make(chan rpcnet.PlainMessage, 1)
	server := startServer(t, plainRegistry(), nil, collector{seen: seen})
	client := mustConnect(t, server, plainRegistry())
	answer, err := client.Invoke(demo.EchoRequestID, demo.EncodeEcho("rpc"))
	if err != nil || answer.Kind != frpc.OutcomeResponse {
		t.Fatalf("RPC works: %v %v", answer, err)
	}
	if err := client.Session().SendPlain(demo.TicksItemID, []byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	client.Pump()
	select {
	case message := <-seen:
		if message.MessageID != demo.TicksItemID || !bytes.Equal(message.Payload, []byte{1, 2, 3}) {
			t.Fatalf("payload, first 5 bytes not read as a header: %+v", message)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("not delivered")
	}
}

type waitForever struct{}

func (waitForever) Poll(time.Time) frpc.Progress { return frpc.Pending() }

func TestAServerGoingAwayCompletesEachPendingCallOnceWithUnavailable(t *testing.T) {
	slow := frpc.Unary("Test.Slow", demo.EchoRequestID, demo.EchoResponseID)
	callee := build(t, demo.Builder().
		Serve(slow, func(frpc.Method, []byte) (frpc.Handler, error) { return waitForever{}, nil }).
		Serve(demo.Upload, func(frpc.Method, []byte) (frpc.Handler, error) { return waitForever{}, nil }))
	server := startServer(t, callee, nil, nil)
	client := mustConnect(t, server, build(t, demo.Builder().Declare(slow).Declare(demo.Upload)))
	first, _ := client.Call(slow.RequestID, demo.EncodeEcho("a"), nil, frpc.WithoutDeadline())
	second, _ := client.Call(demo.UploadOpenID, demo.EncodeUploadOpen("b"), nil, frpc.WithoutDeadline())
	time.Sleep(100 * time.Millisecond)
	client.Pump()
	server.Stop()
	for _, call := range []frpc.CallID{first, second} {
		outcome, err := client.Next(call)
		if err != nil || outcome.Kind != frpc.OutcomeFailed || outcome.Status.Code != frpc.CodeUnavailable {
			t.Fatalf("%s: expected UNAVAILABLE, got %v %v", call, outcome, err)
		}
	}
	client.Pump()
	if client.Core().PendingCount() != 0 {
		t.Fatal("something is left pending")
	}
}

func TestAConflictingSchemaIsRefusedAtConnectTime(t *testing.T) {
	server := startServer(t, nil, nil, nil)
	var messages []fomoxa.Message
	for _, message := range demo.NetSchema().Messages() {
		if message.ID == demo.EchoRequestID {
			message = fomoxa.Message{ID: message.ID, Fingerprint: 0xBAD, Prefixes: []uint64{0xBAD}}
		}
		messages = append(messages, message)
	}
	conflicting, err := fomoxa.NewSchema(0x1234, messages)
	if err != nil {
		t.Fatal(err)
	}
	_, err = connect(t, server, nil, conflicting)
	var refused *rpcnet.ConnectError
	if !errors.As(err, &refused) || refused.Failure != rpcnet.ConnectRefused || refused.Verdict != fomoxa.VerdictConflict {
		t.Fatalf("expected a refusal with reason 2, got %v", err)
	}
}

func processCPU(t *testing.T) time.Duration {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		t.Fatal(err)
	}
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano())
}

func TestAnIdleConnectionSleepsInTheKernel(t *testing.T) {
	server := startServer(t, nil, nil, nil)
	client := mustConnect(t, server, nil)
	if _, err := client.Invoke(demo.EchoRequestID, demo.EncodeEcho("warm")); err != nil {
		t.Fatal(err)
	}
	before := processCPU(t)
	until := time.Now().Add(time.Second)
	wakeups := 0
	for time.Now().Before(until) {
		_, _ = client.Session().Wait(rpcnet.DefaultWaitPolicy(), time.Now())
		client.Pump()
		wakeups++
	}
	spent := processCPU(t) - before
	if wakeups > 12 {
		t.Fatalf("woke %d times in a second", wakeups)
	}
	if spent >= 150*time.Millisecond {
		t.Fatalf("spent %s of CPU idle", spent)
	}
}

func TestUnsentFramesMakeTheWaitIncludeWritabilityBoundedByTheNearestDeadline(t *testing.T) {
	core := frpc.NewCore(demo.DriverRegistry(), frpc.RoleClient, frpc.Config{})
	core.OnReady()
	now := time.Unix(10, 0)
	policy := rpcnet.DefaultWaitPolicy()
	expect := func(want rpcnet.Wait, got rpcnet.Wait, what string) {
		t.Helper()
		if want != got {
			t.Fatalf("%s: expected %+v, got %+v", what, want, got)
		}
	}
	expect(rpcnet.Wait{Timeout: 250 * time.Millisecond}, policy.Plan(core, false, now), "idle")
	_, _ = core.Call(demo.EchoRequestID, demo.EncodeEcho("x"), nil, frpc.Within(40*time.Millisecond), now)
	expect(rpcnet.Wait{Timeout: 40 * time.Millisecond, Writable: true}, policy.Plan(core, false, now), "a frame waits and a deadline runs")
	core.Drain(func(frpc.Outgoing) frpc.Delivery { return frpc.Accepted })
	expect(rpcnet.Wait{Timeout: 40 * time.Millisecond, Writable: true}, policy.Plan(core, true, now), "net congested")
	expect(rpcnet.Wait{Timeout: 40 * time.Millisecond}, policy.Plan(core, false, now), "sent: readable only")
}

func TestTheWaitWakesWhenACongestedSocketBecomesWritable(t *testing.T) {
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan *net.TCPConn, 1)
	go func() {
		conn, _ := listener.AcceptTCP()
		accepted <- conn
	}()
	transport, err := rpcnet.Dial(listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()
	peer := <-accepted
	defer peer.Close()

	chunk := make([]byte, 64*1024)
	for !transport.Backlogged() {
		transport.Send(chunk)
	}
	started := time.Now()
	ready, _ := transport.WaitReady(true, 200*time.Millisecond)
	if ready || time.Since(started) < 150*time.Millisecond {
		t.Fatal("a full socket with nothing to read does not wake the wait early")
	}
	go func() {
		buffer := make([]byte, 1<<20)
		for {
			if _, err := peer.Read(buffer); err != nil {
				return
			}
		}
	}()
	started = time.Now()
	ready, _ = transport.WaitReady(true, 5*time.Second)
	if !ready || time.Since(started) > 2*time.Second {
		t.Fatal("the wait wakes once the peer drains the socket")
	}
}
