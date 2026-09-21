package models

//fomoxa:model codec=rpc
type EchoRequest struct {
	Text string `fomoxa:"string" codec:"rpc"`
}

//fomoxa:model codec=rpc
type EchoResponse struct {
	Text string `fomoxa:"string" codec:"rpc"`
}

//fomoxa:model codec=rpc
type CountRequest struct {
	Count uint32 `fomoxa:"u32" codec:"rpc"`
}

//fomoxa:model codec=rpc
type CountItem struct {
	Value uint32 `fomoxa:"u32" codec:"rpc"`
}
