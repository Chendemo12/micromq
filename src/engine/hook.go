package engine

import (
	"github.com/Chendemo12/micromq/src/proto"
	"github.com/Chendemo12/micromq/src/transfer"
)

// HookHandler 消息处理方法
type HookHandler func(frame *proto.TransferFrame, con transfer.Conn) error

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
	pms      []*proto.PMessage
	stopErr  error // 不回复客户端的原因
}

func (args *ChainArgs) Reset() {
	args.frame = nil
	args.con = nil
	args.producer = nil
	args.rm = nil
	args.pms = nil
	args.resp = nil
	args.stopErr = nil
}

// DoNotReplyClient 不需要回复客户端
func (args *ChainArgs) DoNotReplyClient(err error) {
	args.stopErr = err
}

// ReplyClient 是否需要回复客户端
func (args *ChainArgs) ReplyClient() bool {
	return args.stopErr == nil
}

// DoNotReplyClientReason 不回复客户端的原因
func (args *ChainArgs) DoNotReplyClientReason() error {
	return args.stopErr
}

type FlowHandler func(args *ChainArgs) (stop bool)
