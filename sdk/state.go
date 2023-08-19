package sdk

import (
	"errors"
	"github.com/Chendemo12/micromq/src/proto"
	"sync"
	"time"
)

//goland:noinspection GoUnusedGlobalVariable
var NewQueue = proto.NewQueue

var (
	framePool = proto.NewFramePool()
	hmPool    = NewHCPMPool()
	emPool    = proto.NewCPMPool()
)

//goland:noinspection GoUnusedGlobalVariable
var (
	ErrTopicEmpty           = errors.New("topic is empty")
	ErrConsumerHandlerIsNil = errors.New("consumer messageHandler is nil")
	ErrConsumerUnregistered = errors.New("consumer unregistered")
	ErrConsumerUnconnected  = errors.New("consumer unconnected")
	ErrProducerUnregistered = errors.New("producer unregistered")
	ErrProducerUnconnected  = errors.New("producer unconnected")
)

const (
	DefaultProducerSendInterval = 500 * time.Millisecond
)

//goland:noinspection GoUnusedGlobalVariable
const (
	AllConfirm    = proto.AllConfirm
	NoConfirm     = proto.NoConfirm
	LeaderConfirm = proto.LeaderConfirm
)

type Queue = proto.Queue

type HCPMessagePool struct {
	cpool    *sync.Pool
	ppool    *sync.Pool
	cCounter *proto.Counter
	pCounter *proto.Counter
}

// CMHistoryNum ConsumerMessage 历史数量
func (m *HCPMessagePool) CMHistoryNum() uint64 { return m.cCounter.Value() }

// PMHistoryNum ProducerMessage 历史数量
func (m *HCPMessagePool) PMHistoryNum() uint64 { return m.pCounter.Value() }

func (m *HCPMessagePool) GetCM() *ConsumerMessage {
	v := m.cpool.Get().(*ConsumerMessage)
	v.Reset()
	v.counter = m.cCounter.ValueBeforeIncrement()

	return v
}

func (m *HCPMessagePool) PutCM(v *ConsumerMessage) {
	cv := m.cCounter.Value()
	if cv == 0 || v.counter <= cv {
		v.Reset()
		m.cpool.Put(v)
	}
}

func (m *HCPMessagePool) GetPM() *ProducerMessage {
	v := m.ppool.Get().(*ProducerMessage)
	v.Reset()
	v.counter = m.pCounter.ValueBeforeIncrement()

	return v
}

func (m *HCPMessagePool) PutPM(v *ProducerMessage) {
	cv := m.pCounter.Value()
	if cv == 0 || v.counter <= cv {
		v.Reset()
		m.ppool.Put(v)
	}
}

func NewHCPMPool() *HCPMessagePool {
	p := &HCPMessagePool{
		cpool:    &sync.Pool{},
		ppool:    &sync.Pool{},
		cCounter: proto.NewCounter(),
		pCounter: proto.NewCounter(),
	}

	p.cpool.New = func() any {
		cm := &ConsumerMessage{}
		cm.Reset()
		return cm
	}
	p.ppool.New = func() any {
		pm := &ProducerMessage{}
		pm.Reset()
		return pm
	}

	return p
}
