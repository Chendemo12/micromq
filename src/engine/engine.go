package engine

import (
	"github.com/Chendemo12/fastapi-tool/helper"
	"github.com/Chendemo12/functools/tcp"
	"sync"
)

type Engine struct {
	HistorySize int
	topics      *sync.Map
}

// RangeTopic if false returned, for-loop will stop
func (e *Engine) RangeTopic(fn func(topic *Topic) bool) {
	e.topics.Range(func(key, value any) bool {
		return fn(value.(*Topic))
	})
}

func (e *Engine) AddTopic(name string) *Topic {
	topic := NewTopic(name, e.HistorySize)
	e.topics.Store(name, topic)
	return topic
}

// GetTopic 获取topic,并在不存在时自动新建一个topic
func (e *Engine) GetTopic(name string) *Topic {
	var topic *Topic
	v, ok := e.topics.Load(name)
	if !ok {
		topic = e.AddTopic(name)
	} else {
		topic = v.(*Topic)
	}

	return topic
}

func (e *Engine) GetTopicOffset(name string) uint64 {
	var offset uint64
	e.RangeTopic(func(topic *Topic) bool {
		if topic.Name == name {
			offset = topic.offset
			return false
		}
		return true
	})

	return offset
}

func (e *Engine) SendMessage(msg *Message) uint64 {
	return e.GetTopic(msg.Topic).Product(msg)
}

func (e *Engine) RemoveConsumer(addr string) {
	e.RangeTopic(func(topic *Topic) bool {
		topic.DelConsumer(addr)
		return true
	})
}

func (e *Engine) HandleRegisterMessage(content []byte, r *tcp.Remote) ([]byte, error) {
	msg := &RegisterMessage{}
	err := helper.JsonUnmarshal(content, msg)
	if err != nil {
		return nil, err
	}
	for _, name := range msg.Topics {
		e.GetTopic(name).AddConsumer(&Consumer{
			Conf: &ConsumerConfig{
				Topics: msg.Topics,
				Ack:    msg.Ack,
			},
			Addr: r.Addr(),
			Conn: r,
		})
	}

	return nil, nil
}

func (e *Engine) HandleProductionMessage(content []byte, addr string) ([]byte, error) {
	msg := mPool.Get()
	// TODO： Put(_)
	// TODO: 分离生产者和消费者

	err := helper.JsonUnmarshal(content, msg)
	if err != nil {
		return nil, err
	}

	topic := e.GetTopic(msg.Topic)
	offset := topic.Product(msg)
	consumer := topic.GetConsumer(addr)
	if consumer != nil && consumer.NeedConfirm() {
		// 需要返回确认消息给客户端
		resp := &MessageResponse{
			Result:      true,
			Offset:      offset,
			ReceiveTime: msg.ProductTime,
		}

		return helper.JsonMarshal(resp)
	}

	return nil, nil
}

func New(historySize int) *Engine {
	if historySize == 0 {
		historySize = 100
	}
	return &Engine{
		HistorySize: historySize,
		topics:      &sync.Map{},
	}
}
