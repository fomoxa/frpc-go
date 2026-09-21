//go:build unix

package main

import (
	"fmt"
	"io"
	"os"
	"time"

	fomoxa "github.com/fomoxa/go"

	frpc "github.com/fomoxa/frpc-go"
	"github.com/fomoxa/frpc-go/examples/demo"
	"github.com/fomoxa/frpc-go/rpcnet"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	switch {
	case len(args) == 2 && args[0] == "serve":
		return serve(args[1])
	case len(args) == 2 && args[0] == "drive":
		return drive(args[1])
	case len(args) == 1 && args[0] == "frames":
		for _, line := range demo.Frames() {
			fmt.Println(line)
		}
		return 0
	default:
		fmt.Fprintln(os.Stderr, "usage: frpc-interop serve <host:port> | drive <host:port> | frames")
		return 64
	}
}

func serve(address string) int {
	server, err := rpcnet.Bind(address, demo.NetSchema(), demo.ResponderRegistry(true), fomoxa.Config{}, frpc.Config{})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	done := server.Start()
	fmt.Printf("listening %s\n", server.Addr())
	_, _ = io.Copy(io.Discard, os.Stdin)
	server.Stop()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
	}
	return 0
}

func drive(address string) int {
	client, err := rpcnet.Connect(address, demo.NetSchema(), demo.DriverRegistry(), 5*time.Second, rpcnet.ClientConfig{})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer client.Close()
	failures := demo.Drive(client, os.Stdout)
	if failures == 0 {
		fmt.Println("all checks passed")
		return 0
	}
	fmt.Printf("%d checks failed\n", failures)
	return 1
}
