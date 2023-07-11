package engine

import (
	"github.com/Chendemo12/fastapi-tool/logger"
	"github.com/Chendemo12/synshare-mq/src/proto"
	"os"
	"os/signal"
	"sync"
)

var mPool = proto.NewMessagePool()
var mrPool = proto.NewMessageRespPool()

type Config struct {
	Host        string       `json:"host"`
	Port        string       `json:"port"`
	MaxOpenConn int          `json:"max_open_conn"`
	BufferSize  int          `json:"buffer_size"`
	Logger      logger.Iface `json:"-"`
}

type Engine struct {
	conf      *Config
	producers []*Producer // 生产者
	consumers []*Consumer // 消费者
	topics    *sync.Map
	transfer  *Transfer
}

// RangeTopic if false returned, for-loop will stop
func (e *Engine) RangeTopic(fn func(topic *Topic) bool) {
	e.topics.Range(func(key, value any) bool {
		return fn(value.(*Topic))
	})
}

func (e *Engine) AddTopic(name string) *Topic {
	topic := NewTopic(name, e.conf.BufferSize)
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

// Publisher 发布消息,并返回此消息在当前topic中的偏移量
func (e *Engine) Publisher(msg *proto.Message) uint64 {
	return e.GetTopic(msg.Topic).Publisher(msg)
}

// RemoveConsumer 删除一个消费者
func (e *Engine) RemoveConsumer(addr string) {
	for _, consumer := range e.consumers {
		if consumer == nil || consumer.Conn == nil {
			continue
		}
		if consumer.Addr == addr {
			for _, name := range consumer.Conf.Topics {
				e.GetTopic(name).RemoveConsumer(addr)
			}
		}
	}
}

func (e *Engine) Run() {
	go func() {
		err := e.transfer.Start()
		if err != nil {
			e.conf.Logger.Error("server starts failed, ", err)
			os.Exit(1)
		}
	}()

	// 关闭开关, buffered
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit // 阻塞进程，直到接收到停止信号,准备关闭程序

	e.transfer.Stop()
}

func New(c ...Config) *Engine {
	var d Config

	if len(c) > 0 {
		d = c[0]
	} else {
		d = Config{
			Host:        "127.0.0.1",
			Port:        "9999",
			MaxOpenConn: 50,
			BufferSize:  200,
			Logger:      logger.NewDefaultLogger(),
		}
	}

	if d.BufferSize == 0 {
		d.BufferSize = 100
	}
	if d.Logger == nil {
		d.Logger = logger.NewDefaultLogger()
	}

	if !(d.MaxOpenConn > 0 && d.MaxOpenConn <= 100) {
		d.MaxOpenConn = 50
	}

	eng := &Engine{
		conf: &Config{
			Host:        d.Host,
			Port:        d.Port,
			MaxOpenConn: d.MaxOpenConn,
			BufferSize:  d.BufferSize,
			Logger:      d.Logger,
		},
		producers: make([]*Producer, d.MaxOpenConn),
		consumers: make([]*Consumer, d.MaxOpenConn),
		topics:    &sync.Map{},
	}

	eng.transfer = &Transfer{
		logger: d.Logger,
		mq:     eng,
	}
	eng.transfer.SetEngine(eng).init()

	return eng
}
