package engine

import (
	"github.com/Chendemo12/functools/tcp"
	"github.com/Chendemo12/micromq/src/proto"
)

// HookHandler 消息处理方法
type HookHandler func(frame *proto.TransferFrame, r *tcp.Remote) (goon bool, err error)

type Hook struct {
	Type    proto.MessageType
	Handler HookHandler
	IsAsync bool
}

func emptyHookHandler(_ *proto.TransferFrame, _ *tcp.Remote) (goon bool, err error) {
	return false, nil
}

// ================================== 链式处理请求 ==================================

type ChainArgs struct {
	frame    *proto.TransferFrame
	r        *tcp.Remote
	producer *Producer
	rm       *proto.RegisterMessage
	resp     *proto.MessageResponse
	err      error
	pms      []*proto.PMessage
}

type FlowHandler func(args *ChainArgs) (stop bool)
