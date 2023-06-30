package engine

import "sync"

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

func (e *Engine) RegisterConsumer(con Consumer) {
	for _, name := range con.Topics() {
		e.GetTopic(name).AddConsumer(con)
	}
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

func (e *Engine) ProductMessage(msg *Message) {
	e.GetTopic(msg.Topic).Product(msg.Key, msg.Value)
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
