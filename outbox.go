package frpc

import (
	"math"
	"slices"
)

type Outgoing struct {
	MessageID uint32
	Payload   []byte
}

type Delivery int

const (
	Accepted Delivery = iota
	Rejected
)

type Sink func(frame Outgoing) Delivery

type waitingCredit struct {
	messageID uint32
	items     uint32
}

type Outbox struct {
	queues     map[uint32][]Outgoing
	credits    map[uint32]waitingCredit
	turns      []uint32
	maxPerCall int
	maxTotal   int
	quantum    int
	total      int
}

func NewOutbox(maxPerCall, maxTotal, quantum int) *Outbox {
	return &Outbox{
		queues:     map[uint32][]Outgoing{},
		credits:    map[uint32]waitingCredit{},
		maxPerCall: maxPerCall,
		maxTotal:   maxTotal,
		quantum:    quantum,
	}
}

func (o *Outbox) Len() int { return o.total }

func (o *Outbox) Empty() bool { return o.total == 0 && len(o.credits) == 0 }

func (o *Outbox) WaitingCredit(call CallID) (uint32, bool) {
	waiting, ok := o.credits[uint32(call)]
	return waiting.items, ok
}

func (o *Outbox) Grant(call CallID, messageID uint32, items uint32) {
	already := o.credits[uint32(call)].items
	o.credits[uint32(call)] = waitingCredit{messageID: messageID, items: saturatingAdd(already, items)}
}

func (o *Outbox) QueuedFor(call CallID) int { return len(o.queues[uint32(call)]) }

func (o *Outbox) FullFor(call CallID) bool { return o.QueuedFor(call) >= o.maxPerCall }

func (o *Outbox) HasRoom(call CallID) bool { return o.total < o.maxTotal && !o.FullFor(call) }

func (o *Outbox) RequestStillQueued(call CallID) bool { return o.opensWithRequest(uint32(call)) }

func (o *Outbox) PushTerminal(call CallID, frame Outgoing) { o.append(uint32(call), frame) }

func (o *Outbox) Push(call CallID, frame Outgoing) bool {
	if o.total >= o.maxTotal || o.FullFor(call) {
		return false
	}
	o.append(uint32(call), frame)
	return true
}

func (o *Outbox) Discard(call CallID) {
	key := uint32(call)
	o.total -= len(o.queues[key])
	delete(o.queues, key)
	delete(o.credits, key)
	o.turns = slices.DeleteFunc(o.turns, func(turn uint32) bool { return turn == key })
}

func (o *Outbox) Clear() {
	clear(o.queues)
	clear(o.credits)
	o.turns = nil
	o.total = 0
}

func (o *Outbox) Drain(sink Sink) {
	if !o.drainCredits(sink) {
		return
	}
	o.drainFrames(sink)
	o.drainCredits(sink)
}

func (o *Outbox) append(call uint32, frame Outgoing) {
	queue := o.queues[call]
	if len(queue) == 0 {
		o.turns = append(o.turns, call)
	}
	o.queues[call] = append(queue, frame)
	o.total++
}

func (o *Outbox) opensWithRequest(call uint32) bool {
	queue := o.queues[call]
	return len(queue) > 0 && len(queue[0].Payload) > 0 && KindBits(queue[0].Payload[0]) == KindRequest
}

func (o *Outbox) drainFrames(sink Sink) {
	for o.total > 0 && len(o.turns) > 0 {
		call := o.turns[0]
		o.turns = o.turns[1:]
		spent := 0
		rejected := false
		for spent < o.quantum && len(o.queues[call]) > 0 {
			frame := o.queues[call][0]
			if sink(frame) == Rejected {
				rejected = true
				break
			}
			spent += len(frame.Payload)
			o.queues[call] = o.queues[call][1:]
			o.total--
		}
		if len(o.queues[call]) > 0 {
			o.turns = append(o.turns, call)
		} else {
			delete(o.queues, call)
		}
		if rejected {
			return
		}
	}
}

func (o *Outbox) drainCredits(sink Sink) bool {
	var ready []uint32
	for call := range o.credits {
		if !o.opensWithRequest(call) {
			ready = append(ready, call)
		}
	}
	slices.Sort(ready)
	for _, call := range ready {
		waiting, ok := o.credits[call]
		if !ok {
			continue
		}
		body := NewBodyWriter().PutU32(waiting.items).Bytes()
		payload := Encode(Header{Kind: KindCredit, Call: CallID(call)}, nil, body)
		if sink(Outgoing{MessageID: waiting.messageID, Payload: payload}) == Rejected {
			return false
		}
		delete(o.credits, call)
	}
	return true
}

func saturatingAdd(left, right uint32) uint32 {
	sum := uint64(left) + uint64(right)
	if sum > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(sum)
}
