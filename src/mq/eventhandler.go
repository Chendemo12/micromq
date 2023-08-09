package mq

import (
	"github.com/Chendemo12/functools/tcp"
	"github.com/Chendemo12/micromq/src/engine"
	"github.com/Chendemo12/micromq/src/proto"
)

type CoreEventHandler struct {
	engine.DefaultEventHandler
}

func (e CoreEventHandler) OnFrameParseError(_ *proto.TransferFrame, _ *tcp.Remote) {}

func (e CoreEventHandler) OnConsumerRegister(_ string) {}
func (e CoreEventHandler) OnProducerRegister(_ string) {}
func (e CoreEventHandler) OnConsumerClosed(_ string)   {}
func (e CoreEventHandler) OnProducerClosed(_ string)   {}

func (e CoreEventHandler) OnCMConsumed(record *engine.HistoryRecord) {}

func (e CoreEventHandler) OnNotImplementMessageType(frame *proto.TransferFrame, r *tcp.Remote) (bool, error) {
	return false, nil
}
