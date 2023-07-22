package engine

import (
	"github.com/Chendemo12/functools/tcp"
	"github.com/Chendemo12/synshare-mq/src/proto"
)

type EventHandler interface {
	// OnFrameParseError 当来自客户端消息帧解析出错时触发的事件
	OnFrameParseError(frame *proto.TransferFrame, r *tcp.Remote)
	OnConsumerRegister(addr string)
	OnProducerRegister(addr string)
	OnConsumerClosed(addr string)
	OnProducerClosed(addr string)
	OnNotImplementMessageType(frame *proto.TransferFrame, r *tcp.Remote) (bool, error)
}

type emptyEventHandler struct{}

func (e emptyEventHandler) OnFrameParseError(_ *proto.TransferFrame, _ *tcp.Remote) {}

func (e emptyEventHandler) OnConsumerRegister(_ string) {}
func (e emptyEventHandler) OnProducerRegister(_ string) {}
func (e emptyEventHandler) OnConsumerClosed(_ string)   {}
func (e emptyEventHandler) OnProducerClosed(_ string)   {}

func (e emptyEventHandler) OnNotImplementMessageType(frame *proto.TransferFrame, r *tcp.Remote) (bool, error) {
	return emptyHookHandler(frame, r)
}
