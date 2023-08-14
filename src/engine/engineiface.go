package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/Chendemo12/fastapi-tool/logger"
	"github.com/Chendemo12/micromq/src/proto"
	"github.com/Chendemo12/micromq/src/transfer"
	"reflect"
	"sync"
	"time"
)

func (e *Engine) Ctx() context.Context { return e.conf.Ctx }

func (e *Engine) Logger() logger.Iface { return e.conf.Logger }

func (e *Engine) EventHandler() EventHandler { return e.conf.EventHandler }

// ReplaceTransfer 替换传输层实现
func (e *Engine) ReplaceTransfer(transfer transfer.Transfer) *Engine {
	if transfer != nil {
		e.transfer = transfer
	}
	return e
}

// SetEventHandler 设置事件触发器
func (e *Engine) SetEventHandler(handler EventHandler) *Engine {
	e.conf.EventHandler = handler
	return e
}

// SetTopicHistoryBufferSize 设置topic历史数据缓存大小, 对于修改前已经创建的topic不受影响
//
//	@param size	int 历史数据缓存大小,[1, 10000)
func (e *Engine) SetTopicHistoryBufferSize(size int) *Engine {
	if size > 0 && size < 10000 {
		e.conf.topicHistorySize = size
	}

	return e
}

// QueryConsumer 查询消费者记录, 若未注册则返回nil
func (e *Engine) QueryConsumer(addr string) (*Consumer, bool) {
	var consumer *Consumer = nil
	e.RangeConsumer(func(c *Consumer) bool {
		if c.Addr == addr {
			consumer = c
			return false
		}
		return true
	})

	return consumer, consumer != nil
}

// QueryProducer 查询生产者记录, 若未注册则返回nil
func (e *Engine) QueryProducer(addr string) (*Producer, bool) {
	var producer *Producer = nil
	e.RangeProducer(func(p *Producer) bool {
		if p.Addr == addr {
			producer = p
			return false
		}
		return true
	})

	return producer, producer != nil
}

// RangeConsumer if false returned, for-loop will stop
func (e *Engine) RangeConsumer(fn func(c *Consumer) bool) {
	for i := 0; i < e.conf.MaxOpenConn; i++ {
		// cannot be nil
		if !fn(e.consumers[i]) {
			return
		}
	}
}

// RangeProducer if false returned, for-loop will stop
func (e *Engine) RangeProducer(fn func(p *Producer) bool) {
	for i := 0; i < e.conf.MaxOpenConn; i++ {
		if !fn(e.producers[i]) {
			return
		}
	}
}

// RangeTopic if false returned, for-loop will stop
func (e *Engine) RangeTopic(fn func(topic *Topic) bool) {
	e.topics.Range(func(key, value any) bool {
		return fn(value.(*Topic))
	})
}

// AddTopic 添加一个新的topic,如果topic以存在则跳过
func (e *Engine) AddTopic(name []byte) *Topic {
	topic, _ := e.topics.LoadOrStore(
		string(name), NewTopic(
			name,
			e.conf.BufferSize,
			e.conf.topicHistorySize,
			e.EventHandler().OnCMConsumed,
		),
	)

	return topic.(*Topic)
}

// GetTopic 获取topic,并在不存在时自动新建一个topic
func (e *Engine) GetTopic(name []byte) *Topic {
	var topic *Topic

	v, ok := e.topics.Load(string(name))
	if !ok {
		topic = e.AddTopic(name)
	} else {
		topic = v.(*Topic)
	}

	return topic
}

// GetTopicOffset 查询指定topic当前的消息偏移量
func (e *Engine) GetTopicOffset(name []byte) uint64 {
	var offset uint64

	e.RangeTopic(func(topic *Topic) bool {
		if bytes.Compare(topic.Name, name) == 0 {
			offset = topic.Offset
			return false
		}
		return true
	})

	return offset
}

// RemoveConsumer 删除一个消费者
func (e *Engine) RemoveConsumer(addr string) {
	e.cpLock.Lock()
	defer e.cpLock.Unlock()

	c, exist := e.QueryConsumer(addr)
	if !exist {
		return // consumer not found
	}

	for _, name := range c.Conf.Topics {
		// 从相关 topic 中删除消费者记录
		e.GetTopic([]byte(name)).RemoveConsumer(addr)
	}

	c.reset()
	e.Logger().Info(fmt.Sprintf("connection <%s:%s> removed.", proto.ConsumerLinkType, addr))
	go e.EventHandler().OnConsumerClosed(addr)
}

// RemoveProducer 删除一个生产者
func (e *Engine) RemoveProducer(addr string) {
	e.cpLock.Lock()
	defer e.cpLock.Unlock()

	p, exist := e.QueryProducer(addr)
	if exist {
		p.reset()
		e.Logger().Debug(fmt.Sprintf("connection <%s:%s> removed.", proto.ProducerLinkType, addr))
		go e.EventHandler().OnProducerClosed(addr)
	}
}

// Publisher 发布消息,并返回此消息在当前topic中的偏移量
func (e *Engine) Publisher(msg *proto.PMessage) uint64 {
	return e.GetTopic(msg.Topic).Publisher(msg)
}

// ProducerSendInterval 允许生产者发送数据间隔
func (e *Engine) ProducerSendInterval() time.Duration {
	return e.producerSendInterval
}

// HeartbeatInterval 心跳周期间隔
func (e *Engine) HeartbeatInterval() float64 {
	if e.conf.HeartbeatTimeout == 0 {
		return 30
	}
	return e.conf.HeartbeatTimeout
}

// BindMessageHandler 绑定一个自实现的协议处理器,
//
//	参数m为实现了 proto.Message 接口的协议,
//
//	参数handler则为收到此协议后的同步处理函数, 如果需要在处理完成之后向客户端返回消息,则直接就地修改frame参数,
//		并返回 true 和 nil, 除此之外,则不会向客户端返回任何消息
//		HookHandler 的第一个参数为接收到的消息帧,需自行解码, 第二个参数为当前的客户端连接,
//		此方法需返回(是否返回数据,处理是否正确)两个参数.
//
//	参数texts则为协议m的摘要名称
func (e *Engine) BindMessageHandler(m proto.Message, handler HookHandler, texts ...string) error {
	text := ""
	if len(texts) > 0 {
		text = texts[0]
	} else {
		rt := reflect.TypeOf(m)
		if rt.Kind() == reflect.Ptr {
			rt = rt.Elem()
		}
		text = rt.Name()
	}

	// 添加到协议描述符表
	if proto.GetDescriptor(m.MessageType()).UserDefined() {
		proto.AddDescriptor(m, text)

		e.hooks[m.MessageType()].Type = m.MessageType()
		e.hooks[m.MessageType()].Handler = handler

		return nil
	} else {
		return errors.New("built-in message type cannot be modified")
	}
}

// NeedToken 是否需要密钥认证
func (e *Engine) NeedToken() bool { return e.conf.Token != "" }

// IsTokenCorrect 判断客户端的token是否正确，若未开启token验证，则始终正确
func (e *Engine) IsTokenCorrect(token string) bool {
	if e.conf.Token == "" { // 未设置token验证
		return true
	}
	return e.conf.Token == token
}

// Serve 阻塞运行
func (e *Engine) Serve() error {
	if e.transfer == nil {
		return errors.New("transfer instance is not implemented")
	}

	e.Logger().Debug("broker starting...")
	e.beforeServe()
	e.scheduler.Run()

	if e.NeedToken() {
		e.Logger().Debug("broker token authentication is enabled.")
	}
	return e.transfer.Serve()
}

func (e *Engine) Stop() {
	e.transfer.Stop()
}

// New 创建一个新的服务器
func New(cs ...Config) *Engine {
	conf := &Config{
		Host:             "127.0.0.1",
		Port:             "7270",
		MaxOpenConn:      50,
		BufferSize:       200,
		HeartbeatTimeout: 60,
	}
	if len(cs) > 0 {
		conf.Host = cs[0].Host
		conf.Port = cs[0].Port
		conf.MaxOpenConn = cs[0].MaxOpenConn
		conf.BufferSize = cs[0].BufferSize
		conf.Logger = cs[0].Logger
		conf.Crypto = cs[0].Crypto
		conf.Token = cs[0].Token
		conf.EventHandler = cs[0].EventHandler
		conf.HeartbeatTimeout = cs[0].HeartbeatTimeout
	}

	conf.Clean()
	// 修改全局加解密器
	proto.SetGlobalCrypto(conf.Crypto)

	eng := &Engine{
		conf:                 conf,
		topics:               &sync.Map{},
		transfer:             nil,
		producerSendInterval: 500 * time.Millisecond,
		cpLock:               &sync.RWMutex{},
		argsPool: &sync.Pool{
			New: func() any { return &ChainArgs{} },
		},
	}

	return eng
}
