package engine

import (
	"github.com/Chendemo12/micromq/src/proto"
	"github.com/Chendemo12/micromq/src/transfer"
)

// EventHandler 事件触发器
type EventHandler interface {
	// OnFrameParseError 当来自客户端消息帧解析出错时触发的事件(同步调用)
	OnFrameParseError(frame *proto.TransferFrame, con transfer.Conn)
	// OnConsumerRegister 当消费者注册成功时触发的事件(异步调用)
	OnConsumerRegister(addr string)
	// OnProducerRegister 当生产者注册成功时触发的事件(异步调用)
	OnProducerRegister(addr string)
	// OnConsumerClosed 当消费者关闭连接时触发的事件(异步调用)
	OnConsumerClosed(addr string)
	// OnProducerClosed 当生产者关闭连接时触发的事件(异步调用)
	OnProducerClosed(addr string)
	// OnConsumerHeartbeatTimeout 当消费者心跳超时触发的事件(异步调用)
	OnConsumerHeartbeatTimeout(event TimeoutEvent)
	// OnProducerHeartbeatTimeout 当生产者心跳超时时触发的事件(异步调用)
	OnProducerHeartbeatTimeout(event TimeoutEvent)
	// OnConsumerRegisterTimeout 当消费者连接成功后不注册引发的超时事件(异步调用)
	OnConsumerRegisterTimeout(event TimeoutEvent)
	// OnProducerRegisterTimeout 当消生产者连接成功后不注册引发的超时事件(异步调用)
	OnProducerRegisterTimeout(event TimeoutEvent)
	// OnNotImplementMessageType 当收到一个未实现的消息帧时触发的事件(同步调用)
	OnNotImplementMessageType(frame *proto.TransferFrame, con transfer.Conn) (bool, error)
	// OnCMConsumed 当一个消费者被消费成功(成功发送给全部消费者)后时触发的事件(同步调用)
	OnCMConsumed(record *HistoryRecord)
}

type DefaultEventHandler struct{}

func (e DefaultEventHandler) OnFrameParseError(_ *proto.TransferFrame, _ transfer.Conn) {}

func (e DefaultEventHandler) OnConsumerRegister(_ string) {}
func (e DefaultEventHandler) OnProducerRegister(_ string) {}
func (e DefaultEventHandler) OnConsumerClosed(_ string)   {}
func (e DefaultEventHandler) OnProducerClosed(_ string)   {}

func (e DefaultEventHandler) OnConsumerHeartbeatTimeout(event TimeoutEvent) {}
func (e DefaultEventHandler) OnProducerHeartbeatTimeout(event TimeoutEvent) {}
func (e DefaultEventHandler) OnConsumerRegisterTimeout(event TimeoutEvent)  {}
func (e DefaultEventHandler) OnProducerRegisterTimeout(event TimeoutEvent)  {}

func (e DefaultEventHandler) OnCMConsumed(_ *HistoryRecord) {}

func (e DefaultEventHandler) OnNotImplementMessageType(frame *proto.TransferFrame, con transfer.Conn) (bool, error) {
	return emptyHookHandler(frame, con)
}
