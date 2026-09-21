package frpc

import (
	"errors"
	"fmt"
)

const (
	CodeCancelled          uint32 = 1
	CodeDeadlineExceeded   uint32 = 2
	CodeUnimplemented      uint32 = 3
	CodeInvalidArgument    uint32 = 4
	CodeUnauthenticated    uint32 = 5
	CodePermissionDenied   uint32 = 6
	CodeResourceExhausted  uint32 = 7
	CodeFailedPrecondition uint32 = 8
	CodeUnavailable        uint32 = 9
	CodeInternal           uint32 = 10
)

func CodeName(code uint32) string {
	switch code {
	case CodeCancelled:
		return "CANCELLED"
	case CodeDeadlineExceeded:
		return "DEADLINE_EXCEEDED"
	case CodeUnimplemented:
		return "UNIMPLEMENTED"
	case CodeInvalidArgument:
		return "INVALID_ARGUMENT"
	case CodeUnauthenticated:
		return "UNAUTHENTICATED"
	case CodePermissionDenied:
		return "PERMISSION_DENIED"
	case CodeResourceExhausted:
		return "RESOURCE_EXHAUSTED"
	case CodeFailedPrecondition:
		return "FAILED_PRECONDITION"
	case CodeUnavailable:
		return "UNAVAILABLE"
	case CodeInternal:
		return "INTERNAL"
	default:
		return "UNKNOWN"
	}
}

type Status struct {
	Code    uint32
	Message string
}

func NewStatus(code uint32, message string) *Status {
	return &Status{Code: code, Message: message}
}

func Cancelled() *Status { return NewStatus(CodeCancelled, "cancelled by the caller") }

func DeadlineExceeded() *Status { return NewStatus(CodeDeadlineExceeded, "deadline exceeded") }

func Unimplemented(requestID uint32) *Status {
	return NewStatus(CodeUnimplemented, fmt.Sprintf("no handler for request type 0x%08X", requestID))
}

func InvalidArgument(message string) *Status { return NewStatus(CodeInvalidArgument, message) }

func Unauthenticated(message string) *Status { return NewStatus(CodeUnauthenticated, message) }

func PermissionDenied(message string) *Status { return NewStatus(CodePermissionDenied, message) }

func ResourceExhausted(message string) *Status { return NewStatus(CodeResourceExhausted, message) }

func FailedPrecondition(message string) *Status { return NewStatus(CodeFailedPrecondition, message) }

func Unavailable() *Status {
	return NewStatus(CodeUnavailable, "the session ended before the call completed")
}

func Internal(message string) *Status { return NewStatus(CodeInternal, message) }

func StatusOf(err error) *Status {
	var status *Status
	if errors.As(err, &status) {
		return status
	}
	return Internal(err.Error())
}

func (s *Status) Error() string { return fmt.Sprintf("%s (%s)", s.Message, s.Name()) }

func (s *Status) Name() string { return CodeName(s.Code) }

func (s *Status) Retryable() bool {
	return s.Code == CodeUnavailable || s.Code == CodeResourceExhausted
}

func (s *Status) Encode() []byte {
	return NewBodyWriter().PutU32(s.Code).PutString(s.Message).Bytes()
}

func DecodeStatus(body []byte) *Status {
	reader := NewBodyReader(body)
	code, present, err := reader.ReadU32()
	if err != nil || !present {
		return Internal("error frame carried no status code")
	}
	message, _, err := reader.ReadString()
	if err != nil {
		return Internal("error frame carried an undecodable status message")
	}
	return NewStatus(code, message)
}
