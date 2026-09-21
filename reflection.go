package frpc

const ReflectMethodName = "FRpc.Reflect"

type Reflection struct {
	RequestID  uint32
	ResponseID uint32
	SchemaJSON []byte
}

func (r *Reflection) Method() Method {
	return Unary(ReflectMethodName, r.RequestID, r.ResponseID).Idempotent()
}

func EncodeReflectResponse(methods []Method, schemaJSON []byte) []byte {
	writer := NewBodyWriter().PutU32(uint32(len(methods)))
	for _, method := range methods {
		writer.
			PutString(method.Name).
			PutU32(uint32(method.Shape)).
			PutU32(method.RequestID).
			PutU32(method.CallerItemID()).
			PutU32(method.ReplyID).
			PutU32(uint32(method.RetrySafety))
	}
	return writer.PutBytes(schemaJSON).Bytes()
}
