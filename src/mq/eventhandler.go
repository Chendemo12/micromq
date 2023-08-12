package mq

import (
	"github.com/Chendemo12/micromq/src/engine"
	"github.com/Chendemo12/micromq/src/proto"
	"github.com/Chendemo12/micromq/src/transfer"
)

type CoreEventHandler struct {
	engine.DefaultEventHandler
}

func (e CoreEventHandler) OnFrameParseError(_ *proto.TransferFrame, _ transfer.Conn) {}

func (e CoreEventHandler) OnConsumerRegister(_ string) {}
func (e CoreEventHandler) OnProducerRegister(_ string) {}
func (e CoreEventHandler) OnConsumerClosed(_ string)   {}
func (e CoreEventHandler) OnProducerClosed(_ string)   {}

func (e CoreEventHandler) OnCMConsumed(record *engine.HistoryRecord) {}

func (e CoreEventHandler) OnNotImplementMessageType(frame *proto.TransferFrame, r transfer.Conn) (bool, error) {
	return false, nil
}
