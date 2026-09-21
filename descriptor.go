package frpc

import (
	"fmt"
	"time"
)

type Shape uint32

const (
	ShapeUnary Shape = iota
	ShapeServerStream
	ShapeClientStream
	ShapeBidiStream
)

func (s Shape) CallerStreams() bool { return s == ShapeClientStream || s == ShapeBidiStream }

func (s Shape) CalleeStreams() bool { return s == ShapeServerStream || s == ShapeBidiStream }

type RetrySafety uint32

const (
	RetryUnsafe RetrySafety = iota
	RetryIdempotent
	RetryKeyed
)

func (r RetrySafety) SurvivesUnknownOutcome() bool { return r != RetryUnsafe }

type Method struct {
	Name           string
	Shape          Shape
	RequestID      uint32
	RequestItemID  uint32
	HasRequestItem bool
	ReplyID        uint32
	Permission     string
	RetrySafety    RetrySafety
	IdleLimit      time.Duration
}

func Unary(name string, requestID, responseID uint32) Method {
	return Method{Name: name, Shape: ShapeUnary, RequestID: requestID, ReplyID: responseID}
}

func ServerStream(name string, requestID, itemID uint32) Method {
	return Method{Name: name, Shape: ShapeServerStream, RequestID: requestID, ReplyID: itemID}
}

func ClientStream(name string, requestID, requestItemID, responseID uint32) Method {
	return Method{
		Name:           name,
		Shape:          ShapeClientStream,
		RequestID:      requestID,
		RequestItemID:  requestItemID,
		HasRequestItem: true,
		ReplyID:        responseID,
	}
}

func BidiStream(name string, requestID, requestItemID, replyItemID uint32) Method {
	return Method{
		Name:           name,
		Shape:          ShapeBidiStream,
		RequestID:      requestID,
		RequestItemID:  requestItemID,
		HasRequestItem: true,
		ReplyID:        replyItemID,
	}
}

func (m Method) Requiring(permission string) Method {
	m.Permission = permission
	return m
}

func (m Method) Idempotent() Method {
	m.RetrySafety = RetryIdempotent
	return m
}

func (m Method) Keyed() Method {
	m.RetrySafety = RetryKeyed
	return m
}

func (m Method) IdleAfter(limit time.Duration) Method {
	m.IdleLimit = limit
	return m
}

func (m Method) CallerItemID() uint32 {
	if m.HasRequestItem {
		return m.RequestItemID
	}
	return m.RequestID
}

func (m Method) MessageIDs() []uint32 {
	ids := []uint32{m.RequestID, m.ReplyID}
	if m.HasRequestItem {
		ids = append(ids, m.RequestItemID)
	}
	return ids
}

type HandlerFactory func(method Method, body []byte) (Handler, error)

type Entry struct {
	Method  Method
	Factory HandlerFactory
}

type RegistryError struct {
	Reason string
}

func (e *RegistryError) Error() string { return e.Reason }

const MaxMethods = 65_536

type Registry struct {
	voidID    uint32
	errorID   uint32
	creditID  uint32
	hasCredit bool
	entries   []*Entry
	byRequest map[uint32]*Entry
	idSet     map[uint32]struct{}
	plainIDs  map[uint32]struct{}
}

func (r *Registry) VoidID() uint32 { return r.voidID }

func (r *Registry) ErrorID() uint32 { return r.errorID }

func (r *Registry) CreditID() (uint32, bool) { return r.creditID, r.hasCredit }

func (r *Registry) Methods() []Method {
	methods := make([]Method, 0, len(r.entries))
	for _, entry := range r.entries {
		methods = append(methods, entry.Method)
	}
	return methods
}

func (r *Registry) Owns(messageID uint32) bool {
	_, ok := r.idSet[messageID]
	return ok
}

func (r *Registry) IsPlain(messageID uint32) bool {
	_, ok := r.plainIDs[messageID]
	return ok
}

func (r *Registry) EntryFor(requestID uint32) *Entry { return r.byRequest[requestID] }

func (r *Registry) MethodFor(requestID uint32) (Method, bool) {
	entry := r.byRequest[requestID]
	if entry == nil {
		return Method{}, false
	}
	return entry.Method, true
}

func (r *Registry) RequireDeclaredIn(schemaDeclares func(uint32) bool) error {
	for _, entry := range r.entries {
		for _, messageID := range entry.Method.MessageIDs() {
			if !schemaDeclares(messageID) {
				return &RegistryError{Reason: fmt.Sprintf(
					"method %s uses 0x%08X, which the schema does not declare", entry.Method.Name, messageID)}
			}
		}
	}
	return nil
}

type RegistryBuilder struct {
	voidID     uint32
	errorID    uint32
	creditID   uint32
	hasCredit  bool
	entries    []*Entry
	plainIDs   map[uint32]struct{}
	reflection *Reflection
}

func NewRegistry(voidID, errorID uint32) *RegistryBuilder {
	return &RegistryBuilder{voidID: voidID, errorID: errorID, plainIDs: map[uint32]struct{}{}}
}

func (b *RegistryBuilder) Credit(messageID uint32) *RegistryBuilder {
	b.creditID = messageID
	b.hasCredit = true
	return b
}

func (b *RegistryBuilder) Reflection(requestID, responseID uint32, schemaJSON []byte) *RegistryBuilder {
	b.reflection = &Reflection{RequestID: requestID, ResponseID: responseID, SchemaJSON: schemaJSON}
	return b
}

func (b *RegistryBuilder) Declare(method Method) *RegistryBuilder {
	b.entries = append(b.entries, &Entry{Method: method})
	return b
}

func (b *RegistryBuilder) Serve(method Method, factory HandlerFactory) *RegistryBuilder {
	b.entries = append(b.entries, &Entry{Method: method, Factory: factory})
	return b
}

func (b *RegistryBuilder) PlainMessage(messageID uint32) *RegistryBuilder {
	b.plainIDs[messageID] = struct{}{}
	return b
}

func (b *RegistryBuilder) Build() (*Registry, error) {
	all := append([]*Entry(nil), b.entries...)
	if b.reflection != nil {
		method := b.reflection.Method()
		var served []Method
		for _, entry := range all {
			if entry.Factory != nil {
				served = append(served, entry.Method)
			}
		}
		served = append(served, method)
		answer := EncodeReflectResponse(served, b.reflection.SchemaJSON)
		all = append(all, &Entry{Method: method, Factory: func(Method, []byte) (Handler, error) {
			return RespondNow(answer), nil
		}})
	}
	if len(all) > MaxMethods {
		return nil, &RegistryError{Reason: fmt.Sprintf("%d methods exceeds the %d ceiling", len(all), MaxMethods)}
	}

	byRequest := make(map[uint32]*Entry, len(all))
	idSet := map[uint32]struct{}{b.voidID: {}, b.errorID: {}}
	if b.hasCredit {
		idSet[b.creditID] = struct{}{}
	}
	for _, entry := range all {
		method := entry.Method
		if previous, taken := byRequest[method.RequestID]; taken {
			return nil, &RegistryError{Reason: fmt.Sprintf(
				"methods %s and %s share request type 0x%08X", previous.Method.Name, method.Name, method.RequestID)}
		}
		byRequest[method.RequestID] = entry
		for _, messageID := range method.MessageIDs() {
			if _, plain := b.plainIDs[messageID]; plain {
				return nil, &RegistryError{Reason: fmt.Sprintf(
					"method %s uses 0x%08X, which is also a plain message id", method.Name, messageID)}
			}
			if messageID == b.voidID || messageID == b.errorID || (b.hasCredit && messageID == b.creditID) {
				return nil, &RegistryError{Reason: fmt.Sprintf(
					"method %s uses 0x%08X, reserved for a system model", method.Name, messageID)}
			}
			idSet[messageID] = struct{}{}
		}
	}

	plainIDs := make(map[uint32]struct{}, len(b.plainIDs))
	for id := range b.plainIDs {
		plainIDs[id] = struct{}{}
	}
	return &Registry{
		voidID:    b.voidID,
		errorID:   b.errorID,
		creditID:  b.creditID,
		hasCredit: b.hasCredit,
		entries:   all,
		byRequest: byRequest,
		idSet:     idSet,
		plainIDs:  plainIDs,
	}, nil
}
