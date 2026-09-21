package frpc

import "time"

type ProgressKind int

const (
	ProgressPending ProgressKind = iota
	ProgressRespond
	ProgressItem
	ProgressEnd
	ProgressFail
)

type Progress struct {
	Kind   ProgressKind
	Body   []byte
	Status *Status
}

func Pending() Progress { return Progress{Kind: ProgressPending} }

func Respond(body []byte) Progress { return Progress{Kind: ProgressRespond, Body: body} }

func Item(body []byte) Progress { return Progress{Kind: ProgressItem, Body: body} }

func End() Progress { return Progress{Kind: ProgressEnd} }

func Fail(status *Status) Progress { return Progress{Kind: ProgressFail, Status: status} }

type Handler interface {
	Poll(now time.Time) Progress
}

type Canceller interface {
	Cancel()
}

type ItemReceiver interface {
	Item(body []byte) error
}

type StreamCloser interface {
	EndOfStream() error
}

type Granter interface {
	Grant() uint32
}

type PollFunc func(now time.Time) Progress

func (f PollFunc) Poll(now time.Time) Progress { return f(now) }

type immediate struct {
	progress *Progress
}

func RespondNow(body []byte) Handler {
	progress := Respond(body)
	return &immediate{progress: &progress}
}

func FailNow(status *Status) Handler {
	progress := Fail(status)
	return &immediate{progress: &progress}
}

func (h *immediate) Poll(time.Time) Progress {
	if h.progress == nil {
		return Fail(Internal("handler polled after completion"))
	}
	next := *h.progress
	h.progress = nil
	return next
}

type itemsHandler struct {
	items     [][]byte
	next      int
	cancelled bool
}

func ItemsOf(items [][]byte) Handler { return &itemsHandler{items: items} }

func (h *itemsHandler) Poll(time.Time) Progress {
	if h.cancelled {
		return Fail(Cancelled())
	}
	if h.next >= len(h.items) {
		return End()
	}
	item := h.items[h.next]
	h.next++
	return Item(item)
}

func (h *itemsHandler) Cancel() { h.cancelled = true }
