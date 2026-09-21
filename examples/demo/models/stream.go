package models

//fomoxa:model codec=rpc
type UploadOpen struct {
	Name string `fomoxa:"string" codec:"rpc"`
}

//fomoxa:model codec=rpc
type UploadChunk struct {
	Size uint32 `fomoxa:"u32" codec:"rpc"`
}

//fomoxa:model codec=rpc
type UploadDone struct {
	Summary string `fomoxa:"string" codec:"rpc"`
}

//fomoxa:model codec=rpc
type ChatOpen struct {
	Room string `fomoxa:"string" codec:"rpc"`
}

//fomoxa:model codec=rpc
type ChatSaid struct {
	Line string `fomoxa:"string" codec:"rpc"`
}

//fomoxa:model codec=rpc
type ChatHeard struct {
	Line string `fomoxa:"string" codec:"rpc"`
}

//fomoxa:model codec=rpc
type TicksRequest struct{}

//fomoxa:model codec=rpc
type TicksItem struct {
	Value uint32 `fomoxa:"u32" codec:"rpc"`
}
