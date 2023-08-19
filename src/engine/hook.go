package engine

import (
	"github.com/Chendemo12/micromq/src/proto"
	"github.com/Chendemo12/micromq/src/transfer"
)

// HookHandler 消息处理方法
type HookHandler func(frame *proto.TransferFrame, con transfer.Conn) (goon bool, err error)

type Hook struct {
	Type    proto.MessageType
	Handler HookHandler
	IsAsync bool
}

// ================================== 链式处理请求 ==================================

type ChainArgs struct {
	frame    *proto.TransferFrame
	con      transfer.Conn
	resp     *proto.MessageResponse
	producer *Producer
	rm       *proto.RegisterMessage
	err      error
	pms      []*proto.PMessage
}

func (args *ChainArgs) Reset() {
	args.frame = nil
	args.con = nil
	args.producer = nil
	args.rm = nil
	args.pms = nil
	args.resp = nil
	args.err = nil
}

type FlowHandler func(args *ChainArgs) (stop bool)
