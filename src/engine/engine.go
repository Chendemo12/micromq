package engine

import (
	"bytes"
	"fmt"
	"github.com/Chendemo12/fastapi-tool/logger"
	"github.com/Chendemo12/functools/tcp"
	"github.com/Chendemo12/synshare-mq/src/proto"
	"os"
	"os/signal"
	"sync"
	"time"
)

type Config struct {
	Host         string       `json:"host"`
	Port         string       `json:"port"`
	MaxOpenConn  int          `json:"max_open_conn"` // 允许的最大连接数, 即 生产者+消费者最多有 MaxOpenConn 个
	BufferSize   int          `json:"buffer_size"`   // 生产者消息历史记录最大数量
	Logger       logger.Iface `json:"-"`
	Crypto       proto.Crypto `json:"-"` // 加密器
	EventHandler EventHandler
}

func (c *Config) Clean() *Config {
	if !(c.BufferSize > 0 && c.BufferSize <= 5000) {
		c.BufferSize = 100
	}
	if !(c.MaxOpenConn > 0 && c.MaxOpenConn <= 100) {
		c.MaxOpenConn = 50
	}

	if c.Logger == nil {
		c.Logger = logger.NewDefaultLogger()
	}
	if c.Crypto == nil {
		c.Crypto = proto.DefaultCrypto()
	}

	if c.EventHandler == nil {
		c.EventHandler = emptyEventHandler{}
	}

	return c
}

type Engine struct {
	conf                 *Config
	producers            []*Producer // 生产者
	consumers            []*Consumer // 消费者
	topics               *sync.Map
	transfer             *Transfer
	producerSendInterval time.Duration // 生产者发送消息的时间间隔 = 500ms
	cpLock               *sync.RWMutex // consumer producer lock
	hooks                [256]*Hook
}

func (e *Engine) init() *Engine {
	// 初始化全部内存对象
	e.producers = make([]*Producer, e.conf.MaxOpenConn)
	e.consumers = make([]*Consumer, e.conf.MaxOpenConn)

	for i := 0; i < e.conf.MaxOpenConn; i++ {
		e.consumers[i] = &Consumer{
			index: i,
			mu:    &sync.Mutex{},
			Conf:  &ConsumerConfig{},
			Addr:  "",
			Conn:  nil,
		}

		e.producers[i] = &Producer{
			index: i,
			mu:    &sync.Mutex{},
			Conf:  &ProducerConfig{},
			Addr:  "",
			Conn:  nil,
		}
	}

	for i := 0; i < 256; i++ {
		e.hooks[i] = &Hook{
			Type:       nil,
			Message:    nil,
			Text:       "",
			Fun:        nil,
			UserDefine: true,
		}
	}

	// 修改全局加解密器
	proto.SetGlobalCrypto(e.conf.Crypto)

	return e
}

func (e *Engine) Logger() logger.Iface       { return e.conf.Logger }
func (e *Engine) EventHandler() EventHandler { return e.conf.EventHandler }

// QueryConsumer 查询消费者记录, 若未注册则返回nil
func (e *Engine) QueryConsumer(addr string) (*Consumer, error) {
	index := -1
	e.RangeConsumer(func(c *Consumer) bool {
		if c.Addr == addr {
			index = c.index
			return false
		}
		return true
	})

	if index != -1 {
		return e.consumers[index], nil
	}
	return nil, ErrConsumerNotRegister
}

// QueryProducer 查询生产者记录, 若未注册则返回nil
func (e *Engine) QueryProducer(addr string) (*Producer, error) {
	index := -1
	e.RangeProducer(func(p *Producer) bool {
		if p.Addr == addr {
			index = p.index
			return false
		}
		return true
	})
	if index != -1 {
		return e.producers[index], nil
	}
	return nil, ErrProducerNotRegister
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

func (e *Engine) AddTopic(name []byte) *Topic {
	topic := NewTopic(name, e.conf.BufferSize)
	e.topics.Store(string(name), topic)
	return topic
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
			offset = topic.offset
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

	c, err := e.QueryConsumer(addr)
	if err != nil {
		return // consumer not found
	}

	for _, name := range c.Conf.Topics {
		// 从相关 topic 中删除消费者记录
		e.GetTopic([]byte(name)).RemoveConsumer(addr)
	}

	c.reset()
	e.Logger().Info(fmt.Sprintf("<%s:%s> removed", proto.ConsumerLinkType, addr))
	go e.EventHandler().OnConsumerClosed(addr)
}

func (e *Engine) RemoveProducer(addr string) {
	e.cpLock.Lock()
	defer e.cpLock.Unlock()

	p, err := e.QueryProducer(addr)
	if err != nil {
		return
	}

	p.reset()
	e.Logger().Info(fmt.Sprintf("<%s:%s> removed", proto.ProducerLinkType, addr))
	go e.EventHandler().OnProducerClosed(addr)
}

// IsProducerRegister 依据IP地址判断当前生产者是否已注册
func (e *Engine) IsProducerRegister(addr string) bool {
	_, err := e.QueryProducer(addr)
	return err == nil
}

// Publisher 发布消息,并返回此消息在当前topic中的偏移量
func (e *Engine) Publisher(msg *proto.PMessage) uint64 {
	return e.GetTopic(msg.Topic).Publisher(msg)
}

// ProducerSendInterval 允许生产者发送数据间隔
func (e *Engine) ProducerSendInterval() time.Duration {
	return e.producerSendInterval
}

// Distribute 分发消息
func (e *Engine) Distribute(frame *proto.TransferFrame, r *tcp.Remote) {
	defer framePool.Put(frame)

	var err error
	var needResp bool

	switch frame.Type {
	case proto.RegisterMessageType: // 注册消费者
		// 内部会就地修改 frame
		needResp, err = e.HandleRegisterMessage(frame, r)

	case proto.PMessageType: // 生产消息
		// 内部会就地修改 frame
		needResp, err = e.HandleProductionMessage(frame, r)
	}

	// 错误，或不需要回写返回值
	if err != nil || !needResp {
		return
	}

	// 重新构建并写入消息帧
	_bytes, err := frame.Build()
	if err != nil { // 此处构建失败的可能性为0, 为了保持接口一致而为之
		return
	}
	_, err = r.Write(_bytes)
	err = r.Drain()
	if err != nil {
		e.Logger().Warn(fmt.Sprintf(
			"send <message:%d> to '%s' failed: %s", frame.Type, r.Addr(), err,
		))
		return
	}
}

// HandleRegisterMessage 处理注册消息, 内部无需返回消息,通过修改frame实现返回消息
func (e *Engine) HandleRegisterMessage(frame *proto.TransferFrame, r *tcp.Remote) (bool, error) {
	msg, err := frame.ParseTo()
	rgm, ok := msg.(*proto.RegisterMessage)
	if err != nil || !ok {
		return false, fmt.Errorf("register message parse failed, %v", err)
	}

	e.Logger().Debug(fmt.Sprintf("receive '%s' from  %s", rgm, r.Addr()))

	result := false // 是否允许注册

	switch rgm.Type {
	case proto.ProducerLinkType: // 注册生产者
		// 上个锁, 防止刚注册就断开
		e.cpLock.Lock()
		e.RangeProducer(func(p *Producer) bool {
			if p.Addr == "" { // 记录生产者, 用于判断其后是否要返回消息投递后的确认消息
				p.Addr = r.Addr()
				p.Conf = &ProducerConfig{Ack: rgm.Ack, TickerInterval: e.ProducerSendInterval()}
				p.Conn = r
				result = true

				return false
			}
			return true
		})

		e.cpLock.Unlock()

	case proto.ConsumerLinkType: // 注册消费者
		e.RangeConsumer(func(c *Consumer) bool {
			if c.Addr == "" {
				c.Addr = r.Addr()
				c.Conf = &ConsumerConfig{Topics: rgm.Topics, Ack: rgm.Ack}
				c.setConn(r)
				result = true

				for _, name := range rgm.Topics {
					e.GetTopic([]byte(name)).AddConsumer(c)
				}

				return false
			}
			return true
		})

		e.cpLock.Unlock()
	}

	// 无论如何都需要构建返回值
	resp := &proto.MessageResponse{
		Result:         result,
		Offset:         0,
		ReceiveTime:    time.Now(),
		TickerInterval: e.ProducerSendInterval(),
	}
	frame.Type = proto.RegisterMessageRespType
	frame.Data, err = resp.Build()
	if err != nil {
		return false, fmt.Errorf("register response message build failed: %v", err)
	}

	e.Logger().Info(fmt.Sprintf("<%s:%s> registered", rgm.Type, r.Addr()))

	// 触发回调
	if result && rgm.Type == proto.ProducerLinkType {
		go e.EventHandler().OnProducerRegister(r.Addr())
	}
	if result && rgm.Type == proto.ConsumerLinkType {
		go e.EventHandler().OnConsumerRegister(r.Addr())
	}

	return true, nil
}

// HandleProductionMessage 处理生产者消息帧，此处需要判断生产者是否已注册
// 内部无需返回消息,通过修改frame实现返回消息
func (e *Engine) HandleProductionMessage(frame *proto.TransferFrame, r *tcp.Remote) (bool, error) {
	if !e.IsProducerRegister(r.Addr()) {
		// 返回令客户端重新注册命令
		e.Logger().Debug("found unregister producer, let re-register: ", r.Addr())
		e.LetReRegister(r)
		return false, ErrProducerNotRegister
	}

	// 会存在多个消息封装为一个帧
	pms := make([]*proto.PMessage, 0)
	stream := bytes.NewReader(frame.Data)

	// 循环解析生产者消息
	var err error
	for err == nil && stream.Len() > 0 {
		pm := mp.GetPM()
		err = pm.ParseFrom(stream)
		if err == nil {
			pms = append(pms, pm)
		}
	}

	if len(pms) < 1 {
		return false, ErrPMNotFound
	}

	// 若是批量发送数据,则取最后一条消息的偏移量
	var offset uint64 = 0
	for _, pm := range pms {
		offset = e.Publisher(pm)
	}

	consumer := e.consumers[r.Index()]
	if consumer != nil && consumer.NeedConfirm() {
		// 需要返回确认消息给客户端
		resp := &proto.MessageResponse{
			Result:      true,
			Offset:      offset,
			ReceiveTime: time.Now(),
		}
		_bytes, err2 := resp.Build()
		if err2 != nil {
			e.Logger().Warn("response build failed: ", err2)
			return false, fmt.Errorf("response build failed: %v", err2)
		}
		frame.Type = proto.MessageRespType
		frame.Data = _bytes
		return true, nil

	} else {
		// 不需要返回值
		return false, nil
	}
}

// LetReRegister 令客户端重新发起注册流程
func (e *Engine) LetReRegister(r *tcp.Remote) {
	frame := framePool.Get()
	defer framePool.Put(frame)

	// 重新发起注册暂无消息体
	_bytes, err := frame.BuildWith(proto.ReRegisterMessageType, []byte{})
	if err != nil {
		e.Logger().Warn("make re-register failed: ", err)
	} else {
		_, _ = r.Write(_bytes)
		_ = r.Drain()
	}
}

// Run 阻塞运行
func (e *Engine) Run() {
	e.init()

	go func() {
		err := e.transfer.Start()
		if err != nil {
			e.Logger().Error("server starts failed: ", err)
			os.Exit(1)
		}
	}()

	// 关闭开关, buffered
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit // 阻塞进程，直到接收到停止信号,准备关闭程序

	e.transfer.Stop()
}

// New 创建一个新的服务器
func New(cs ...Config) *Engine {
	conf := &Config{
		Host:        "127.0.0.1",
		Port:        "7270",
		MaxOpenConn: 50,
		BufferSize:  200,
	}
	if len(cs) > 0 {
		conf.Host = cs[0].Host
		conf.Port = cs[0].Port
		conf.MaxOpenConn = cs[0].MaxOpenConn
		conf.BufferSize = cs[0].BufferSize
		conf.Logger = cs[0].Logger
		conf.Crypto = cs[0].Crypto
		conf.EventHandler = cs[0].EventHandler
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
	}

	eng.transfer = &Transfer{logger: conf.Logger, mq: eng}
	eng.transfer.SetEngine(eng)

	return eng
}
