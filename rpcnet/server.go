//go:build unix

package rpcnet

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	fomoxa "github.com/fomoxa/go"

	frpc "github.com/fomoxa/frpc-go"
)

type ServerObserver interface {
	Opened(connection uint64, peer net.Addr)
	Outcome(connection uint64, outcome frpc.Outcome)
	Plain(connection uint64, message PlainMessage)
	Closed(connection uint64)
}

type NopObserver struct{}

func (NopObserver) Opened(uint64, net.Addr) {}

func (NopObserver) Outcome(uint64, frpc.Outcome) {}

func (NopObserver) Plain(uint64, PlainMessage) {}

func (NopObserver) Closed(uint64) {}

type Server struct {
	listener       *net.TCPListener
	schema         *fomoxa.Schema
	registry       *frpc.Registry
	netConfig      fomoxa.Config
	config         frpc.Config
	policy         WaitPolicy
	observer       ServerObserver
	configure      func(*frpc.Core)
	nextConnection atomic.Uint64
	stopped        atomic.Bool
	connections    sync.WaitGroup
}

func Bind(address string, schema *fomoxa.Schema, registry *frpc.Registry, netConfig fomoxa.Config, config frpc.Config) (*Server, error) {
	if err := RequireDeclared(registry, schema); err != nil {
		return nil, err
	}
	resolved, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return nil, err
	}
	listener, err := net.ListenTCP("tcp", resolved)
	if err != nil {
		return nil, err
	}
	return &Server{
		listener:  listener,
		schema:    schema,
		registry:  registry,
		netConfig: netConfig,
		config:    config,
		policy:    DefaultWaitPolicy(),
		observer:  NopObserver{},
	}, nil
}

func (s *Server) Addr() *net.TCPAddr { return s.listener.Addr().(*net.TCPAddr) }

func (s *Server) Stopped() bool { return s.stopped.Load() }

func (s *Server) WithPolicy(policy WaitPolicy) *Server {
	s.policy = policy.normalized()
	return s
}

func (s *Server) Observing(observer ServerObserver) *Server {
	s.observer = observer
	return s
}

func (s *Server) Configuring(configure func(*frpc.Core)) *Server {
	s.configure = configure
	return s
}

func (s *Server) Start() <-chan error {
	done := make(chan error, 1)
	go func() { done <- s.Run() }()
	return done
}

func (s *Server) Run() error {
	for !s.stopped.Load() {
		conn, err := s.listener.AcceptTCP()
		if err != nil {
			if s.stopped.Load() || errors.Is(err, net.ErrClosed) {
				return nil
			}
			continue
		}
		if s.stopped.Load() {
			_ = conn.Close()
			return nil
		}
		connection := s.nextConnection.Add(1)
		s.connections.Add(1)
		go func() {
			defer s.connections.Done()
			s.serve(connection, conn)
		}()
	}
	return nil
}

func (s *Server) Stop() {
	s.stopped.Store(true)
	_ = s.listener.Close()
}

func (s *Server) Wait() { s.connections.Wait() }

func (s *Server) serve(connection uint64, conn *net.TCPConn) {
	transport, err := NewSocketTransport(conn)
	if err != nil {
		_ = conn.Close()
		return
	}
	session, err := NewSession(transport, s.schema, s.registry, frpc.RoleServer, s.netConfig, s.config)
	if err != nil {
		transport.Close()
		return
	}
	s.observer.Opened(connection, transport.RemoteAddr())
	if s.configure != nil {
		s.configure(session.Core())
	}
	for !s.stopped.Load() {
		session.Tick(time.Now())
		s.report(connection, session)
		if session.Closed() {
			break
		}
		if _, err := session.Wait(s.policy, time.Now()); err != nil {
			break
		}
	}
	session.Close()
	s.report(connection, session)
	s.observer.Closed(connection)
}

func (s *Server) report(connection uint64, session *Session) {
	for _, outcome := range session.TakeOutcomes() {
		s.observer.Outcome(connection, outcome)
	}
	for _, message := range session.TakePlain() {
		s.observer.Plain(connection, message)
	}
}
