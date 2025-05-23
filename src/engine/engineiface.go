package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/Chendemo12/functools/logger"
	"github.com/Chendemo12/micromq/src/proto"
	"github.com/Chendemo12/micromq/src/transfer"
)

func (e *Engine) Ctx() context.Context { return e.conf.Ctx }

func (e *Engine) Logger() logger.Iface { return e.conf.Logger }

func (e *Engine) EventHandler() EventHandler { return e.conf.EventHandler }

func (e *Engine) Stat() *Statistic { return e.stat }

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
//	@param	size	int	历史数据缓存大小,[1, 10000)
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

// RangeConsumer 遍历连接的消费者, if false returned, for-loop will stop
func (e *Engine) RangeConsumer(fn func(c *Consumer) bool) {
	for i := 0; i < len(e.consumers); i++ {
		if e.consumers[i].IsFree() {
			continue
		}
		// cannot be nil
		if !fn(e.consumers[i]) {
			return
		}
	}
}

// RangeProducer 遍历连接的生产者, if false returned, for-loop will stop
func (e *Engine) RangeProducer(fn func(p *Producer) bool) {
	for i := 0; i < len(e.producers); i++ {
		if e.producers[i].IsFree() {
			continue
		}
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

// AddTopic 添加一个新的topic
func (e *Engine) AddTopic(name []byte) *Topic {
	nt := NewTopic(
		name,
		e.conf.BufferSize,
		e.conf.topicHistorySize,
	)
	nt.SetOnConsumed(e.EventHandler().OnCMConsumed)
	nt.SetCrypto(e.Crypto())

	e.topics.Store(string(name), nt)

	return nt
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

// TopicExist 判断topic是否存在
func (e *Engine) TopicExist(name string) bool {
	_, ok := e.topics.Load(name)
	return ok
}

// FindTopic 查找topic
func (e *Engine) FindTopic(name string) (*Topic, bool) {
	v, ok := e.topics.Load(name)
	if ok {
		return v.(*Topic), true
	}
	return nil, false
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
		topic, est := e.FindTopic(name)
		if !est {
			continue
		}
		topic.RemoveConsumer(addr)
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

// BindMessageHandler 绑定一个自实现的消息处理器,
//
//	参数handler为收到此消息后的同步处理函数, 如果需要在处理完成之后向客户端返回消息,则直接就地修改frame对象,
//		HookHandler 的第一个参数为接收到的消息帧, 可通过 proto.TransferFrame.Unmarshal 方法解码, 第二个参数为当前的客户端连接,
//		此方法返回"处理是否正确"一个参数, 若定义了 needAck 则需要返回错误消息给客户端
//
//	@param	m		proto.Message	实现了	proto.Message	接口的自定义消息,	不允许与内置消息冲突,	可通过	proto.IsMessageDefined	判断
//	@param	handler	HookHandler		消息处理方法
//	@param	ack		proto.Message	是否需要返回响应给客户端
//	@param	text	string			此消息的摘要名称
func (e *Engine) BindMessageHandler(m proto.Message, ack proto.Message, handler HookHandler, text string) error {
	if text == "" {
		rt := reflect.TypeOf(m)
		if rt.Kind() == reflect.Ptr {
			rt = rt.Elem()
		}
		text = rt.Name()
	}

	// 添加到协议描述符表
	if proto.IsMessageDefined(m.MessageType()) {
		proto.AddDescriptor(m, ack, text)

		e.hooks[m.MessageType()].Type = m.MessageType()
		e.hooks[m.MessageType()].Handler = handler
		e.hooks[m.MessageType()].ACKDefined = ack != nil

		return nil
	} else {
		return errors.New("built-in message type cannot be modified")
	}
}

// SetCrypto 修改全局加解密器, 必须在 Serve 之前设置
func (e *Engine) SetCrypto(crypto proto.Crypto) *Engine {
	if crypto != nil {
		e.crypto = crypto
	}

	return e
}

// SetCryptoPlan 设置加密方案
//
//	@param	option	string		加密方案,	支持token/no	(令牌加密和不加密)
//	@param	key		[]string	其他加密参数
func (e *Engine) SetCryptoPlan(option string, key ...string) *Engine {
	args := append([]string{e.conf.Token}, key...)
	e.crypto = proto.CreateCrypto(option, args...)

	return e
}

// Crypto 全局加解密器
func (e *Engine) Crypto() proto.Crypto { return e.crypto }

// TokenCrypto Token加解密器，亦可作为全局加解密器
func (e *Engine) TokenCrypto() *proto.TokenCrypto { return e.tokenCrypto }

// NeedToken 是否需要密钥认证
func (e *Engine) NeedToken() bool { return e.tokenCrypto.Token != "" }

// IsTokenCorrect 判断客户端的token是否正确，若未开启token验证，则始终正确
func (e *Engine) IsTokenCorrect(token string) bool {
	if e.tokenCrypto.Token == "" { // 未设置token验证
		return true
	}
	return e.tokenCrypto.Token == token
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
	e.Logger().Debug("broker global crypto: ", e.crypto.String())
	return e.transfer.Serve()
}

func (e *Engine) Stop() { e.transfer.Stop() }

// New 创建一个新的服务器
func New(cs ...Config) *Engine {
	conf := &Config{
		Host:             "127.0.0.1",
		Port:             "7270",
		MaxOpenConn:      100,
		BufferSize:       200,
		HeartbeatTimeout: 60,
	}
	if len(cs) > 0 {
		conf.Host = cs[0].Host
		conf.Port = cs[0].Port
		conf.MaxOpenConn = cs[0].MaxOpenConn
		conf.BufferSize = cs[0].BufferSize
		conf.Logger = cs[0].Logger
		conf.Token = cs[0].Token
		conf.EventHandler = cs[0].EventHandler
		conf.HeartbeatTimeout = cs[0].HeartbeatTimeout
	}

	conf.clean()
	eng := &Engine{
		conf:                 conf,
		topics:               &sync.Map{},
		transfer:             nil,
		producerSendInterval: 500 * time.Millisecond,
		cpLock:               &sync.RWMutex{},
		crypto:               proto.DefaultCrypto(),
	}

	return eng
}
