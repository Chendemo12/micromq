package sdk

import (
	"errors"
	"github.com/Chendemo12/synshare-mq/src/proto"
	"sync"
)

var framePool = proto.NewFramePool()
var mPool = NewMessagePool()

var ErrTopicEmpty = errors.New("topic is empty")
var ErrConsumerHandlerIsNil = errors.New("consumer handler is nil")

func NewMessagePool() *MessagePool {
	p := &MessagePool{cpool: &sync.Pool{}, ppool: &sync.Pool{}}

	p.cpool.New = func() any {
		m := &proto.ConsumerMessage{}
		m.Reset()
		return m
	}

	p.ppool.New = func() any {
		m := &proto.ProducerMessage{}
		m.Reset()
		return m
	}

	return p
}

type MessagePool struct {
	cpool *sync.Pool
	ppool *sync.Pool
}

func (m *MessagePool) GetCM() *proto.ConsumerMessage {
	return m.cpool.Get().(*proto.ConsumerMessage)
}

func (m *MessagePool) GetPM() *proto.ProducerMessage {
	return m.ppool.Get().(*proto.ProducerMessage)
}

func (m *MessagePool) PutPM(v *proto.ProducerMessage) {
	m.ppool.Put(v)
}

func (m *MessagePool) PutCM(v *proto.ConsumerMessage) {
	m.cpool.Put(v)
}
