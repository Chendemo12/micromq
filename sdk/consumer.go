package sdk

import (
	"github.com/Chendemo12/synshare-mq/src/engine"
	"sync"
)

type MessageRecord = engine.Message
type ConsumerRegisterMessage = engine.RegisterMessage

type ConsumerHandler interface {
	Topics() []string
	Handler(record *MessageRecord)
}

type Consumer struct {
	link *Link
}

func NewConsumer(conf Config, handler ConsumerHandler) *Consumer {
	con := &Consumer{link: &Link{
		conf: &Config{
			Host: conf.Host,
			Port: conf.Port,
		},
		conn:        nil,
		handler:     nil,
		isConnected: false,
		isRegister:  false,
		mu:          &sync.Mutex{},
	}}
	return con
}
