# frpc-go

fRPC for Go: the RPC layer on top of Fomoxa, written from the [fRPC design](../frpc). Where this code and the design disagree, the design decides and this code changes.

It is the third implementation, after [frpc-rust](../frpc-rust) and [frpc-csharp](../frpc-csharp). All three exchange identical bytes. This test suite checks the frames against a recording from frpc-rust, and [fomoxa/interop](https://github.com/fomoxa/interop) runs the three against each other.

## Layout

| Path | Contents |
|---|---|
| `.` (package `frpc`) | The pure core: wire format, ctx block, registry, send queues and scheduler, interceptors, credit, compression, retry, reflection. No goroutines, no clock, no I/O |
| `rpcnet` | The shell over [fomoxa-go](https://github.com/fomoxa/go): a TCP transport with a wait step, `Session`, a blocking `Client`, and a `Server` with one goroutine per connection |
| `examples/demo` | The demo schema (the same 19 models as the other two implementations), the codecs `fomoxac` generated from it, the methods all three serve, and the shared scenario |
| `cmd/frpc-interop` | The Go half of the cross-language test, used by [fomoxa/interop](https://github.com/fomoxa/interop) |
| `tools/fomoxac.sh` | Runs `fomoxac generate` in a package directory (see Building and testing) |

The module depends on `github.com/fomoxa/go` v0.1.0 and on `golang.org/x/sys` for `poll(2)`. It needs no change to fomoxa-go (design R12). `rpcnet` builds on Unix only.

## Using it

A method is identified by the message id of its request model (design §3.3), and ids come from the codecs `fomoxac` generated:

```go
say := frpc.Unary("Echo.Say", generated.EchoRequestRpcMessageID, generated.EchoResponseRpcMessageID)

registry, err := frpc.NewRegistry(generated.FRpcVoidRpcMessageID, generated.FRpcErrorRpcMessageID).
    Credit(generated.FRpcCreditRpcMessageID).
    Serve(say, func(_ frpc.Method, body []byte) (frpc.Handler, error) {
        return frpc.RespondNow(shout(body)), nil
    }).
    Reflection(generated.FRpcReflectRequestRpcMessageID, generated.FRpcReflectResponseRpcMessageID, schemaJSON).
    Build()

server, err := rpcnet.Bind("127.0.0.1:9431", schema, registry, fomoxa.Config{}, frpc.Config{})
server.Run()
```

```go
client, err := rpcnet.Connect("127.0.0.1:9431", schema, callerRegistry, 5*time.Second, rpcnet.ClientConfig{})
outcome, err := client.InvokeWith(generated.EchoRequestRpcMessageID, body, nil, frpc.Within(2*time.Second))
```

`Build` returns a `*frpc.RegistryError` for the registration checks of design §4.10. `Bind` and `Connect` also refuse a registry whose descriptors name a message the schema does not declare.

A handler implements `Poll(now)` and returns `frpc.Pending()`, `Respond`, `Item`, `End` or `Fail`. A stream handler may also implement `Item(body) error` to accept caller items, `EndOfStream() error` to hear the end of the caller's stream, `Grant() uint32` to grant credit for the caller direction, and `Cancel()` to hear a cancellation. A factory or handler refuses a call by returning a `*frpc.Status`. Any other error, and any panic, becomes `INTERNAL` for that call alone and does not reach the session.

Zero values mean the defaults: `frpc.Config{}` takes the ceilings of design §4.8, `rpcnet.WaitPolicy{}` the 250 ms idle ceiling, and `frpc.CallOptions{}` a call with no deadline. A zero `time.Time` means no deadline, as it does for `net.Conn.SetDeadline`.

The API has three levels, matching the two driving modes of design §19:

| Type | Who drives it |
|---|---|
| `frpc.Core` | The application, with an injected `now`. Frames in through `OnMessage`, frames out through `Drain`. This is the level every in-memory test uses |
| `rpcnet.Session` | The application, calling `Tick(now)` and `Wait(policy, now)` in its own loop |
| `rpcnet.Client`, `rpcnet.Server` | The library. `Client` blocks the calling goroutine until an outcome arrives; `Server` runs one goroutine per connection, blocked in `poll(2)` while idle |

One goroutine drives each session (design §19, rule 1), so a `Client` or `Session` is not safe for concurrent use. Handlers run on the goroutine that drives their connection.

### The wait step

The wait step has to know whether bytes are being held back, to choose between waiting for readable and waiting for readable or writable (design §19). fomoxa-go's `Conn` does not report that, so the shell brings its own `SocketTransport`. Its `Backlogged` flag is set while bytes from a partial send are still unsent or the last send was refused, and `Session.Wait` uses it. The wait itself is `poll(2)` on the socket; an idle connection wakes about four times a second.

## Interoperability

`fomoxac` 0.2.1 generated the Go demo schema from Go struct tags. All 19 messages agree with the Rust and C# schemas on id and prefix fingerprints, and the whole-schema fingerprint is `0xB8F0E2543FC9C60D` on all three, so any two of them complete the handshake on its first comparison. Field names follow the C# spelling (`Text`, `DeadlineMs`, `RequestId`), which `fomoxac` normalises to the same fingerprint as Rust's `text`, `deadline_ms` and `request_id`.

`frpc-interop` takes the same commands as the other two:

| Command | What it does |
|---|---|
| `serve <host:port>` | Serves `Echo.Say`, `Echo.Count`, `Blob.Upload`, `Room.Chat`, `Clock.Ticks` and `FRpc.Reflect`, prints `listening <address>`, runs until stdin closes |
| `drive <host:port>` | Runs eleven checks against any fRPC server: unary, server stream, client stream, bidi, their error paths, credit, a stream deadline, `UNIMPLEMENTED` for a type the server does not serve, and reflection |
| `frames` | Two cores exchange one fixed scenario in memory; prints every frame as hex |

```
frpc-rust$ cargo run --features net --example interop -- serve 127.0.0.1:9521
frpc-go$   go run ./cmd/frpc-interop drive 127.0.0.1:9521
```

`go test ./examples/demo` checks the 25 frames of the `frames` scenario line for line against `examples/demo/testdata/frames.txt`, the output of `fomoxa-rpc` 0.1.0 recorded once (see `testdata/SOURCE`). The recording is part of this repository, so the check needs no other toolchain and no network, and a commit to another repository cannot change it.

Running the implementations against each other lives in [fomoxa/interop](https://github.com/fomoxa/interop), under `frpc/`, where each peer depends on released versions only. Its Go peer imports `examples/demo` from this module at a tagged version. It checks every client against every server across Rust, C# and Go, and version skew in both directions.

frpcurl works against the Go server too, since reflection returns the Go `schema.json`:

```
$ frpcurl 127.0.0.1:9523 load
loaded 6 methods and 19 models from 127.0.0.1:9523
$ frpcurl 127.0.0.1:9523 call Echo.Say -d '{"Text":"xin chào"}'
{
  "Text": "XIN CHÀO"
}
```

## Building and testing

```
go build ./...
go test ./...                 # all 99
go test -run Credit .         # tests whose name contains "Credit"
go test -race ./...
```

The models are regenerated by hand:

```
go generate ./examples/demo
```

The directive runs `tools/fomoxac.sh`, which calls `fomoxac generate` in the package's directory. `fomoxac`'s Go backend computes import paths from a `go.mod` in its working directory, and the schema a package embeds (`.fomoxa/schema.json`) has to sit inside that package, so the script writes a `go.mod` naming the package's import path for the length of the run and removes it afterwards. The generated `handshake.go` is left as `fomoxac` writes it, so `gofmt -l` lists it.

## License

Apache-2.0, see [LICENSE](LICENSE).
