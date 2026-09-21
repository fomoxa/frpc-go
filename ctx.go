package frpc

import "crypto/sha256"

type Ctx struct {
	DeadlineMs     uint32
	TraceID        []byte
	SpanID         []byte
	Token          string
	Tenant         string
	IdempotencyKey []byte
	InitialCredit  uint32
}

func WithDeadlineMs(deadlineMs uint32) *Ctx { return &Ctx{DeadlineMs: deadlineMs} }

func WithInitialCredit(initialCredit uint32) *Ctx { return &Ctx{InitialCredit: initialCredit} }

func KeyOf(body []byte) []byte {
	sum := sha256.Sum256(body)
	return sum[:]
}

func KeyedByContent(body []byte) *Ctx { return &Ctx{IdempotencyKey: KeyOf(body)} }

func CtxForMethod(method Method, body []byte) *Ctx {
	if method.RetrySafety == RetryKeyed {
		return KeyedByContent(body)
	}
	return &Ctx{}
}

func (c Ctx) HasDeadline() bool { return c.DeadlineMs != 0 }

func (c Ctx) IsEmpty() bool {
	return c.DeadlineMs == 0 &&
		len(c.TraceID) == 0 &&
		len(c.SpanID) == 0 &&
		c.Token == "" &&
		c.Tenant == "" &&
		len(c.IdempotencyKey) == 0 &&
		c.InitialCredit == 0
}

func (c Ctx) Encode() []byte {
	return NewBodyWriter().
		PutU32(c.DeadlineMs).
		PutBytes(c.TraceID).
		PutBytes(c.SpanID).
		PutString(c.Token).
		PutString(c.Tenant).
		PutBytes(c.IdempotencyKey).
		PutU32(c.InitialCredit).
		Bytes()
}

func DecodeCtx(bytes []byte) (Ctx, error) {
	reader := NewBodyReader(bytes)
	var ctx Ctx
	var present bool
	var err error
	if ctx.DeadlineMs, present, err = reader.ReadU32(); err != nil || !present {
		return ctx, err
	}
	if ctx.TraceID, present, err = reader.ReadBytes(); err != nil || !present {
		return ctx, err
	}
	if ctx.SpanID, present, err = reader.ReadBytes(); err != nil || !present {
		return ctx, err
	}
	if ctx.Token, present, err = reader.ReadString(); err != nil || !present {
		return ctx, err
	}
	if ctx.Tenant, present, err = reader.ReadString(); err != nil || !present {
		return ctx, err
	}
	if ctx.IdempotencyKey, present, err = reader.ReadBytes(); err != nil || !present {
		return ctx, err
	}
	ctx.InitialCredit, _, err = reader.ReadU32()
	return ctx, err
}
