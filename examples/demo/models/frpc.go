package models

//fomoxa:model codec=rpc
type FRpcVoid struct{}

//fomoxa:model codec=rpc
type FRpcError struct {
	Code    uint32 `fomoxa:"u32" codec:"rpc"`
	Message string `fomoxa:"string" codec:"rpc"`
}

//fomoxa:model codec=rpc
type FRpcCredit struct {
	Items uint32 `fomoxa:"u32" codec:"rpc"`
}

//fomoxa:model codec=rpc
type FRpcCtx struct {
	DeadlineMs     uint32 `fomoxa:"u32" codec:"rpc"`
	TraceId        []byte `fomoxa:"bytes" codec:"rpc"`
	SpanId         []byte `fomoxa:"bytes" codec:"rpc"`
	Token          string `fomoxa:"string" codec:"rpc"`
	Tenant         string `fomoxa:"string" codec:"rpc"`
	IdempotencyKey []byte `fomoxa:"bytes" codec:"rpc"`
	InitialCredit  uint32 `fomoxa:"u32" codec:"rpc"`
}

//fomoxa:model codec=rpc
type FRpcReflectRequest struct{}

//fomoxa:model codec=rpc
type FRpcMethodInfo struct {
	Name         string `fomoxa:"string" codec:"rpc"`
	Shape        uint32 `fomoxa:"u32" codec:"rpc"`
	RequestId    uint32 `fomoxa:"u32" codec:"rpc"`
	CallerItemId uint32 `fomoxa:"u32" codec:"rpc"`
	ReplyId      uint32 `fomoxa:"u32" codec:"rpc"`
	RetrySafety  uint32 `fomoxa:"u32" codec:"rpc"`
}

//fomoxa:model codec=rpc
type FRpcReflectResponse struct {
	Methods    []FRpcMethodInfo `fomoxa:"Array<FRpcMethodInfo>" codec:"rpc"`
	SchemaJson []byte           `fomoxa:"bytes" codec:"rpc"`
}
