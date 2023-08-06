package engine

import (
	"bytes"
	"fmt"
	"github.com/Chendemo12/fastapi-tool/logger"
	"github.com/Chendemo12/functools/tcp"
	"github.com/Chendemo12/micromq/src/proto"
	"sync"
	"time"
)

type Config struct {
	Host             string       `json:"host"`
	Port             string       `json:"port"`
	MaxOpenConn      int          `json:"max_open_conn"` // 允许的最大连接数, 即 生产者+消费者最多有 MaxOpenConn 个
	BufferSize       int          `json:"buffer_size"`   // 生产者消息历史记录最大数量
	Logger           logger.Iface `json:"-"`
	Crypto           proto.Crypto `json:"-"` // 加密器
	Token            string       `json:"-"` // 注册认证密钥
	EventHandler     EventHandler // 事件触发器
	topicHistorySize int          // topic 历史缓存大小
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
	c.topicHistorySize = 100

	return c
}

type Engine struct {
	conf                 *Config
	producers            []*Producer // 生产者
	consumers            []*Consumer // 消费者
	topics               *sync.Map
	transfer             Transfer
	producerSendInterval time.Duration // 生产者发送消息的时间间隔 = 500ms
	cpLock               *sync.RWMutex // consumer producer add/remove lock
	// 各种协议的处理者
	hooks        [proto.TotalNumberOfMessages]*Hook
	registerFlow []FlowHandler
	pmFlow       []FlowHandler
	argsPool     *sync.Pool
}

func (e *Engine) beforeServe() *Engine {
	// 初始化全部内存对象
	for i := 0; i < proto.TotalNumberOfMessages; i++ {
		e.hooks[i] = &Hook{ // 初始化为未实现
			Type:    proto.NotImplementMessageType,
			Handler: emptyHookHandler,
		}
	}

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

	// 修改全局加解密器
	proto.SetGlobalCrypto(e.conf.Crypto)

	e.bindProtoHandler()
	e.bindTransfer()
	return e
}

// 注册传输层实现
func (e *Engine) bindTransfer() *Engine {
	// 设置默认实现
	if e.transfer == nil {
		e.transfer = &TCPTransfer{}
	}

	e.transfer.SetHost(e.conf.Host)
	e.transfer.SetPort(e.conf.Port)
	e.transfer.SetMaxOpenConn(e.conf.MaxOpenConn)
	e.transfer.SetLogger(e.Logger())

	e.transfer.SetOnConnectedHandler(e.whenClientAccept)
	e.transfer.SetOnClosedHandler(e.whenClientClose)
	e.transfer.SetOnReceivedHandler(e.distribute)
	e.transfer.SetOnFrameParseErrorHandler(e.EventHandler().OnFrameParseError)

	return e
}

// 绑定处理器
func (e *Engine) bindProtoHandler() *Engine {
	e.registerFlow = []FlowHandler{
		e.registerParser,
		e.registerAuth,
		e.registerAllow,
		e.registerCallback,
	}

	e.pmFlow = []FlowHandler{
		e.producerNotFound,
		e.pmParser,
		e.pmPublisher,
	}

	// 登陆注册
	e.hooks[proto.RegisterMessageType].Type = proto.RegisterMessageType
	e.hooks[proto.RegisterMessageType].Handler = e.registerHandler

	// 生产者消息
	e.hooks[proto.PMessageType].Type = proto.PMessageType
	e.hooks[proto.PMessageType].Handler = e.handlePMessage
	//e.hooks[proto.PMessageType].Handler = e.pmHandler

	return e
}

// 连接成功时不关联数据, 仅在注册成功时,关联到 Engine 中
func (e *Engine) whenClientAccept(r *tcp.Remote) {}

// 连接关闭，删除记录
func (e *Engine) whenClientClose(addr string) {
	e.RemoveConsumer(addr)
	e.RemoveProducer(addr)
}

// 查找一个空闲的 生产者槽位，若未找到则返回 -1，应在查找之前主动加锁
func (e *Engine) findProducerSlot() int {
	for i := 0; i < e.conf.MaxOpenConn; i++ {
		// cannot be nil
		if e.producers[i].Addr == "" {
			return i
		}
	}
	return -1
}

// 查找一个空闲的 消费者槽位，若未找到则返回 -1，应在查找之前主动加锁
func (e *Engine) findConsumerSlot() int {
	for i := 0; i < e.conf.MaxOpenConn; i++ {
		if e.consumers[i].Addr == "" {
			return i
		}
	}
	return -1
}

func (e *Engine) getArgs(frame *proto.TransferFrame, r *tcp.Remote) *ChainArgs {
	args := e.argsPool.Get().(*ChainArgs)
	args.frame = frame
	args.r = r

	return args
}

func (e *Engine) putArgs(args *ChainArgs) {
	args.frame = nil
	args.r = nil
	args.producer = nil
	args.rm = nil
	args.pms = nil
	args.resp = nil
	args.err = nil

	e.argsPool.Put(args)
}

// 将一系列处理过程组合成一条链
func (e *Engine) handlerFlow(args *ChainArgs, links []FlowHandler) (bool, error) {
	defer e.putArgs(args)

	for _, link := range links {
		stop := link(args)
		if stop { // 此环节决定终止后续流程
			return false, args.err
		}
	}

	// 不需要返回响应
	if args.resp == nil {
		return false, args.err
	}

	// 需要返回响应，构建返回值
	args.frame.Data, args.err = args.resp.Build()
	if args.err != nil {
		return false, fmt.Errorf("register response message build failed: %v", args.err)
	}

	return true, nil
}

// 分发消息
func (e *Engine) distribute(frame *proto.TransferFrame, r *tcp.Remote) {
	var err error
	var needResp bool

	if proto.GetDescriptor(frame.Type).MessageType() != proto.NotImplementMessageType {
		// 协议已实现
		needResp, err = e.hooks[frame.Type].Handler(frame, r)
	} else {
		// 此协议未注册, 通过事件回调处理
		needResp, err = e.EventHandler().OnNotImplementMessageType(frame, r)
	}

	// 错误，或不需要回写返回值
	if err != nil || !needResp {
		return
	}

	// 重新构建并写入消息帧
	_bytes, err := frame.Build()
	if err != nil { // 此处构建失败的可能性很小，存在加密错误
		e.Logger().Warn(fmt.Sprintf("build frame <message:%d> failed: %s", frame.Type, err))
		return
	}

	_, err = r.Write(_bytes)
	err = r.Drain()
	if err != nil {
		e.Logger().Warn(fmt.Sprintf(
			"send <message:%d> to '%s' failed: %s", frame.Type, r.Addr(), err,
		))
	}
}

// Deprecated:
func (e *Engine) handleRegisterMessage(frame *proto.TransferFrame, r *tcp.Remote) (bool, error) {
	msg := &proto.RegisterMessage{}

	err := frame.Unmarshal(msg)
	if err != nil {
		// 注册消息帧解析失败，令重新发起注册
		frame.Type = proto.ReRegisterMessageType
		frame.Data = []byte{} // 重新发起注册暂无消息体
		return true, fmt.Errorf("register message parse failed, %v, let re-register: %s", err, r.Addr())
	}

	e.Logger().Debug(fmt.Sprintf("receive '%s' from  %s", msg, r.Addr()))

	status := proto.RefusedStatus
	if e.NeedToken() && e.conf.Token != msg.Token {
		// 需要认证，但是密钥不正确
		status = proto.TokenIncorrectStatus
	} else { // 密钥验证通过
		switch msg.Type {

		case proto.ProducerLinkType: // 注册生产者
			e.cpLock.Lock() // 上个锁, 防止刚注册就断开

			e.RangeProducer(func(p *Producer) bool {
				if p.Addr == "" { // 记录生产者, 用于判断其后是否要返回消息投递后的确认消息
					p.Addr = r.Addr()
					p.Conf = &ProducerConfig{Ack: msg.Ack, TickerInterval: e.ProducerSendInterval()}
					p.Conn = r
					status = proto.AcceptedStatus
					return false
				}
				return true
			})
			e.cpLock.Unlock()

		case proto.ConsumerLinkType: // 注册消费者
			e.cpLock.Lock()

			e.RangeConsumer(func(c *Consumer) bool {
				if c.Addr == "" {
					c.Addr = r.Addr()
					c.Conf = &ConsumerConfig{Topics: msg.Topics, Ack: msg.Ack}
					c.setConn(r)
					status = proto.AcceptedStatus

					for _, name := range msg.Topics {
						e.GetTopic([]byte(name)).AddConsumer(c)
					}
					return false
				}
				return true
			})
			e.cpLock.Unlock()
		}
	}

	// 输出日志
	switch status {
	case proto.TokenIncorrectStatus:
		e.Logger().Info(r.Addr(), " has wrong token, refused.")
	case proto.AcceptedStatus:
		e.Logger().Info(fmt.Sprintf("<%s:%s> registered.", msg.Type, r.Addr()))
	case proto.RefusedStatus:
		e.Logger().Info(r.Addr(), " register refused.")
	}

	// 无论如何都需要构建返回值
	resp := &proto.MessageResponse{
		Status:         status,
		Offset:         0,
		ReceiveTime:    time.Now(),
		TickerInterval: e.ProducerSendInterval(),
	}
	frame.Type = proto.RegisterMessageRespType
	frame.Data, err = resp.Build()

	if err != nil {
		return false, fmt.Errorf("register response message build failed: %v", err)
	}

	// 触发回调
	if resp.Accepted() && msg.Type == proto.ProducerLinkType {
		go e.EventHandler().OnProducerRegister(r.Addr())
	}
	if resp.Accepted() && msg.Type == proto.ConsumerLinkType {
		go e.EventHandler().OnConsumerRegister(r.Addr())
	}

	return true, nil
}

// 处理注册消息, 内部无需返回消息,通过修改frame实现返回消息
func (e *Engine) registerHandler(frame *proto.TransferFrame, r *tcp.Remote) (bool, error) {
	args := e.getArgs(frame, r)
	args.rm = &proto.RegisterMessage{}

	return e.handlerFlow(args, e.registerFlow)
}

// Deprecated:
func (e *Engine) handlePMessage(frame *proto.TransferFrame, r *tcp.Remote) (bool, error) {
	producer, exist := e.QueryProducer(r.Addr())

	if !exist {
		e.Logger().Debug("found unregister producer, let re-register: ", r.Addr())

		// 重新发起注册暂无消息体
		frame.Type = proto.ReRegisterMessageType
		frame.Data = []byte{}

		// 返回令客户端重新注册命令
		return true, nil
	}

	// 存在多个消息封装为一个帧
	pms := make([]*proto.PMessage, 0)
	stream := bytes.NewReader(frame.Data)

	// 循环解析生产者消息
	var err error
	for err == nil && stream.Len() > 0 {
		pm := cpmp.GetPM()
		err = pm.ParseFrom(stream)
		if err == nil {
			pms = append(pms, pm)
		} else {
			cpmp.PutPM(pm)
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

	if producer.NeedConfirm() {
		// 需要返回确认消息给客户端
		resp := &proto.MessageResponse{
			Status:      proto.AcceptedStatus,
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
	}

	return producer.NeedConfirm(), nil
}

// 处理生产者消息帧，此处需要判断生产者是否已注册
// 内部无需返回消息,通过修改frame实现返回消息
func (e *Engine) pmHandler(frame *proto.TransferFrame, r *tcp.Remote) (bool, error) {
	args := e.getArgs(frame, r)

	return e.handlerFlow(args, e.pmFlow)
}
