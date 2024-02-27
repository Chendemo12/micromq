package mq

import (
	"github.com/Chendemo12/micromq/src/engine"
)

type CoreEventHandler struct {
	engine.DefaultEventHandler
}

func (e CoreEventHandler) OnConsumerRegister(_ string) {}
func (e CoreEventHandler) OnProducerRegister(_ string) {}
func (e CoreEventHandler) OnConsumerClosed(_ string)   {}
func (e CoreEventHandler) OnProducerClosed(_ string)   {}
