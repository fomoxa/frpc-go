package frpc

import "math"

type Role int

const (
	RoleClient Role = iota
	RoleServer
)

func (r Role) Allocates(call CallID) bool {
	if r == RoleClient {
		return call.Even()
	}
	return !call.Even()
}

type CallIDAllocator struct {
	role Role
	next uint32
}

func NewCallIDAllocator(role Role) *CallIDAllocator {
	allocator := &CallIDAllocator{role: role}
	if role == RoleServer {
		allocator.next = 1
	}
	return allocator
}

func (a *CallIDAllocator) Role() Role { return a.role }

func (a *CallIDAllocator) Allocate(inUse func(CallID) bool) (CallID, bool) {
	for attempt := uint64(0); attempt <= math.MaxUint32/2; attempt++ {
		candidate := CallID(a.next)
		a.next += 2
		if !inUse(candidate) {
			return candidate, true
		}
	}
	return 0, false
}
