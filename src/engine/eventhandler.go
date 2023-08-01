package engine

import (
	"github.com/Chendemo12/functools/tcp"
	"github.com/Chendemo12/micromq/src/proto"
)

// EventHandler 事件触发器
type EventHandler interface {
	// OnFrameParseError 当来自客户端消息帧解析出错时触发的事件
	OnFrameParseError(frame *proto.TransferFrame, r *tcp.Remote)
	// OnConsumerRegister 当消费者注册成功时触发的事件
	OnConsumerRegister(addr string)
	// OnProducerRegister 当生产者注册成功时触发的事件
	OnProducerRegister(addr string)
	// OnConsumerClosed 当消费者关闭连接时触发的事件
	OnConsumerClosed(addr string)
	// OnProducerClosed 当生产者关闭连接时触发的事件
	OnProducerClosed(addr string)
	// OnNotImplementMessageType 当收到一个未实现的消息帧时触发的事件
	OnNotImplementMessageType(frame *proto.TransferFrame, r *tcp.Remote) (bool, error)
	// OnCMConsumed 当一个消费者被消费成功(成功发送给全部消费者)后时触发的事件
	OnCMConsumed(record *HistoryRecord)
}

type emptyEventHandler struct{}

func (e emptyEventHandler) OnFrameParseError(_ *proto.TransferFrame, _ *tcp.Remote) {}

func (e emptyEventHandler) OnConsumerRegister(_ string) {}
func (e emptyEventHandler) OnProducerRegister(_ string) {}
func (e emptyEventHandler) OnConsumerClosed(_ string)   {}
func (e emptyEventHandler) OnProducerClosed(_ string)   {}

func (e emptyEventHandler) OnCMConsumed(record *HistoryRecord) {}

func (e emptyEventHandler) OnNotImplementMessageType(frame *proto.TransferFrame, r *tcp.Remote) (bool, error) {
	return emptyHookHandler(frame, r)
}
