//go:build unix

package rpcnet

import (
	"errors"
	"fmt"
	"time"

	fomoxa "github.com/fomoxa/go"

	frpc "github.com/fomoxa/frpc-go"
)

type ConnectFailure int

const (
	ConnectRefused ConnectFailure = iota
	ConnectClosed
	ConnectTimedOut
)

type ConnectError struct {
	Failure ConnectFailure
	Verdict fomoxa.Verdict
	Reason  string
}

func (e *ConnectError) Error() string { return e.Reason }

var ErrNoFurtherOutcome = errors.New("rpcnet: the call has no further outcome")

type ClientConfig struct {
	Net    fomoxa.Config
	RPC    frpc.Config
	Policy WaitPolicy
}

type Client struct {
	session *Session
	policy  WaitPolicy
	backlog []frpc.Outcome
}

func Connect(address string, schema *fomoxa.Schema, registry *frpc.Registry, timeout time.Duration, config ClientConfig) (*Client, error) {
	transport, err := Dial(address, timeout)
	if err != nil {
		return nil, err
	}
	session, err := NewSession(transport, schema, registry, frpc.RoleClient, config.Net, config.RPC)
	if err != nil {
		transport.Close()
		return nil, err
	}
	client := &Client{session: session, policy: config.Policy.normalized()}
	giveUp := time.Now().Add(timeout)
	for {
		client.Pump()
		if session.Ready() {
			return client, nil
		}
		if verdict, refused := session.Refusal(); refused {
			client.Close()
			return nil, &ConnectError{Failure: ConnectRefused, Verdict: verdict,
				Reason: fmt.Sprintf("rpcnet: the handshake failed: %s", verdict)}
		}
		if session.Closed() {
			client.Close()
			return nil, &ConnectError{Failure: ConnectClosed, Reason: "rpcnet: the server closed the session"}
		}
		if !time.Now().Before(giveUp) {
			client.Close()
			return nil, &ConnectError{Failure: ConnectTimedOut, Reason: "rpcnet: no handshake before the timeout"}
		}
		if _, err := session.Wait(client.policy, time.Now()); err != nil {
			client.Close()
			return nil, err
		}
	}
}

func (c *Client) Session() *Session { return c.session }

func (c *Client) Core() *frpc.Core { return c.session.Core() }

func (c *Client) Call(requestID uint32, body []byte, ctx *frpc.Ctx, options frpc.CallOptions) (frpc.CallID, error) {
	call, err := c.session.Call(requestID, body, ctx, options, time.Now())
	if err == nil {
		c.Pump()
	}
	return call, err
}

func (c *Client) Invoke(requestID uint32, body []byte) (frpc.Outcome, error) {
	return c.InvokeWith(requestID, body, nil, frpc.DefaultCallOptions())
}

func (c *Client) InvokeWith(requestID uint32, body []byte, ctx *frpc.Ctx, options frpc.CallOptions) (frpc.Outcome, error) {
	call, err := c.Call(requestID, body, ctx, options)
	if err != nil {
		return frpc.Outcome{}, fmt.Errorf("rpcnet: the call could not be sent: %w", err)
	}
	return c.Next(call)
}

func (c *Client) Next(call frpc.CallID) (frpc.Outcome, error) {
	for {
		for index, outcome := range c.backlog {
			if outcome.Call == call {
				c.backlog = append(c.backlog[:index], c.backlog[index+1:]...)
				return outcome, nil
			}
		}
		if _, pending := c.session.Core().PendingRequestID(call); !pending {
			return frpc.Outcome{}, fmt.Errorf("%w: %s", ErrNoFurtherOutcome, call)
		}
		if _, err := c.session.Wait(c.policy, time.Now()); err != nil {
			return frpc.Outcome{}, err
		}
		c.Pump()
	}
}

func (c *Client) Collect(call frpc.CallID) ([]frpc.Outcome, error) {
	var outcomes []frpc.Outcome
	for {
		outcome, err := c.Next(call)
		if err != nil {
			return outcomes, err
		}
		outcomes = append(outcomes, outcome)
		if outcome.Terminates() {
			return outcomes, nil
		}
	}
}

func (c *Client) SendItem(call frpc.CallID, body []byte) error {
	for {
		err := c.session.Core().SendItem(call, body)
		if !errors.Is(err, frpc.ErrNoCredit) && !errors.Is(err, frpc.ErrCongested) {
			c.Pump()
			return err
		}
		if c.session.Closed() {
			return frpc.ErrNotReady
		}
		if _, waitErr := c.session.Wait(c.policy, time.Now()); waitErr != nil {
			return waitErr
		}
		c.Pump()
	}
}

func (c *Client) CloseSend(call frpc.CallID) error {
	err := c.session.Core().CloseSend(call)
	c.Pump()
	return err
}

func (c *Client) Grant(call frpc.CallID, items uint32) error {
	err := c.session.Core().Grant(call, items)
	c.Pump()
	return err
}

func (c *Client) Cancel(call frpc.CallID) {
	c.session.Core().Cancel(call)
	c.Pump()
}

func (c *Client) Pump() {
	c.session.Tick(time.Now())
	c.backlog = append(c.backlog, c.session.TakeOutcomes()...)
}

func (c *Client) Close() { c.session.Close() }
