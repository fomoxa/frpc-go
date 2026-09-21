package frpc

import "time"

type RetryPolicy struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Retryable      map[uint32]bool
}

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:    3,
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     2 * time.Second,
		Retryable:      map[uint32]bool{CodeUnavailable: true, CodeResourceExhausted: true},
	}
}

func NeverRetry() RetryPolicy {
	return RetryPolicy{MaxAttempts: 1, Retryable: map[uint32]bool{}}
}

func (p RetryPolicy) BackoffFor(attempt int) time.Duration {
	doubling := min(max(attempt-1, 0), 16)
	scaled := p.InitialBackoff * time.Duration(int64(1)<<doubling)
	return min(scaled, p.MaxBackoff)
}

type GiveUp int

const (
	GiveUpNotRetryable GiveUp = iota
	GiveUpUnsafeToRepeat
	GiveUpAttemptsExhausted
	GiveUpBudgetExhausted
	GiveUpStreamingCall
)

type RetryDecision struct {
	Retry   bool
	RetryAt time.Time
	Reason  GiveUp
}

type RetryState struct {
	attempt  int
	deadline time.Time
}

func NewRetryState(now time.Time, budget time.Duration) *RetryState {
	state := &RetryState{attempt: 1}
	if budget > 0 {
		state.deadline = now.Add(budget)
	}
	return state
}

func (s *RetryState) Attempt() int { return s.attempt }

func (s *RetryState) Deadline() (time.Time, bool) { return s.deadline, !s.deadline.IsZero() }

func (s *RetryState) Remaining(now time.Time) (time.Duration, bool) {
	if s.deadline.IsZero() {
		return 0, false
	}
	return max(s.deadline.Sub(now), 0), true
}

func (s *RetryState) OnFailure(policy RetryPolicy, status *Status, method Method, now time.Time) RetryDecision {
	if method.Shape != ShapeUnary {
		return RetryDecision{Reason: GiveUpStreamingCall}
	}
	if !policy.Retryable[status.Code] {
		return RetryDecision{Reason: GiveUpNotRetryable}
	}
	if status.Code == CodeUnavailable && !method.RetrySafety.SurvivesUnknownOutcome() {
		return RetryDecision{Reason: GiveUpUnsafeToRepeat}
	}
	if s.attempt >= policy.MaxAttempts {
		return RetryDecision{Reason: GiveUpAttemptsExhausted}
	}
	backoff := policy.BackoffFor(s.attempt)
	if !s.deadline.IsZero() && !now.Add(backoff).Before(s.deadline) {
		return RetryDecision{Reason: GiveUpBudgetExhausted}
	}
	s.attempt++
	return RetryDecision{Retry: true, RetryAt: now.Add(backoff)}
}
