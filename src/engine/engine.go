package engine

import (
	"context"
	"fmt"
	"github.com/Chendemo12/fastapi-tool/cronjob"
	"github.com/Chendemo12/fastapi-tool/logger"
	"github.com/Chendemo12/micromq/src/proto"
	"github.com/Chendemo12/micromq/src/transfer"
	"sync"
	"time"
)

type Config struct {
	Host             string          `json:"host"`
	Port             string          `json:"port"`
	MaxOpenConn      int             `json:"max_open_conn"` // 允许的最大连接数, 即 生产者+消费者最多有 MaxOpenConn 个
	BufferSize       int             `json:"buffer_size"`   // 生产者消息历史记录最大数量
	HeartbeatTimeout float64         `json:"heartbeat_timeout"`
	Logger           logger.Iface    `json:"-"`
	Crypto           proto.Crypto    `json:"-"` // 加解密器
	Token            string          `json:"-"` // 注册认证密钥
	EventHandler     EventHandler    `json:"-"` // 事件触发器
	Ctx              context.Context `json:"-"`
	topicHistorySize int             // topic 历史缓存大小
}

func (c *Config) clean() *Config {
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
		c.EventHandler = DefaultEventHandler{}
	}
	if c.Ctx == nil {
		c.Ctx = context.Background()
	}

	c.topicHistorySize = 100

	return c
}

type Engine struct {
	conf                 *Config
	producers            []*Producer // 生产者
	consumers            []*Consumer // 消费者
	transfer             transfer.Transfer
	topics               *sync.Map
	monitor              *Monitor
	scheduler            *cronjob.Scheduler
	ePool                *EPool             // 池化各种数据
	tokenCrypto          *proto.TokenCrypto // 用于注册消息加解密
	producerSendInterval time.Duration      // 生产者发送消息的时间间隔 = 500ms
	// 各种协议的处理者
	hooks [proto.TotalNumberOfMessages]*Hook
	// 消息帧处理链，每一个链内部无需直接向客户端写入消息,通过修改frame实现返回消息
	flows [proto.TotalNumberOfMessages][]FlowHandler
	// consumer producer add/remove lock
	cpLock *sync.RWMutex
}

func (e *Engine) beforeServe() *Engine {
	// 初始化全部内存对象
	for i := 0; i < proto.TotalNumberOfMessages; i++ {
		// 初始化为未实现
		e.hooks[i] = &Hook{
			Type:    proto.NotImplementMessageType,
			Handler: e.defaultHookHandler,
		}
		// 初始化一个空流程链
		e.flows[i] = make([]FlowHandler, 0)
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

	// 监视器
	e.monitor = &Monitor{broker: e}
	e.scheduler = cronjob.NewScheduler(e.Ctx(), e.Logger())
	e.scheduler.AddCronjob(e.monitor)
	// 初始化池
	e.ePool = &EPool{
		args: &sync.Pool{
			New: func() any { return &ChainArgs{} },
		},
		mResp: &sync.Pool{New: func() any {
			return &proto.MessageResponse{
				Status:      proto.RefusedStatus,
				Offset:      0,
				ReceiveTime: time.Now().Unix(),
			}
		}},
	}
	// 修改加解密器
	e.tokenCrypto = &proto.TokenCrypto{Token: e.conf.Token}

	e.bindMessageHandler()
	e.bindTransfer()
	return e
}

// 注册传输层实现
func (e *Engine) bindTransfer() *Engine {
	e.transfer.SetHost(e.conf.Host)
	e.transfer.SetPort(e.conf.Port)
	e.transfer.SetMaxOpenConn(e.conf.MaxOpenConn)
	e.transfer.SetLogger(e.Logger())

	e.transfer.SetOnConnectedHandler(e.whenClientConnected)
	e.transfer.SetOnClosedHandler(e.whenClientClosed)
	e.transfer.SetOnReceivedHandler(e.distribute)
	e.transfer.SetOnFrameParseErrorHandler(e.EventHandler().OnFrameParseError)

	return e
}

// 注册协议，绑定处理器
func (e *Engine) bindMessageHandler() *Engine {

	// 登陆注册
	e.hooks[proto.RegisterMessageType].Type = proto.RegisterMessageType
	e.flows[proto.RegisterMessageType] = []FlowHandler{
		e.registerParser,
		e.registerAuth,
		e.registerAllow,
		e.registerCallback,
	}

	// 生产者消息
	e.hooks[proto.PMessageType].Type = proto.PMessageType
	e.flows[proto.PMessageType] = []FlowHandler{
		e.producerNotFound,
		e.pmParser,
		e.pmPublisher,
	}

	// 心跳保活
	e.hooks[proto.HeartbeatMessageType].Type = proto.HeartbeatMessageType
	e.flows[proto.HeartbeatMessageType] = []FlowHandler{
		e.receiveHeartbeat,
	}

	return e
}

// 连接成功时不关联数据, 仅在注册成功时,关联到 Engine 中
func (e *Engine) whenClientConnected(con transfer.Conn) {
	// 记录连接，用于判断是否连接成功后不注册
	e.monitor.OnClientConnected(con.Addr())
}

// 连接关闭，删除记录
func (e *Engine) whenClientClosed(addr string) {
	e.RemoveConsumer(addr)
	e.RemoveProducer(addr)

	e.monitor.OnClientClosed(addr)
}

// 查找一个空闲的 生产者槽位，若未找到则返回 -1，应在查找之前主动加锁
func (e *Engine) findProducerSlot() int {
	for i := 0; i < e.conf.MaxOpenConn; i++ {
		// cannot be nil
		if e.producers[i].IsFree() {
			return i
		}
	}
	return -1
}

// 查找一个空闲的 消费者槽位，若未找到则返回 -1，应在查找之前主动加锁
func (e *Engine) findConsumerSlot() int {
	for i := 0; i < e.conf.MaxOpenConn; i++ {
		if e.consumers[i].IsFree() {
			return i
		}
	}
	return -1
}

// 将一系列处理过程组合成一条链
func (e *Engine) flowToHandler(args *ChainArgs, messageType proto.MessageType) (bool, error) {
	defer e.ePool.putArgs(args)

	for _, link := range e.flows[messageType] {
		if link(args) { // 此环节决定终止后续流程
			break
		}
	}

	// 构建返回值
	args.frame.Data, args.err = args.resp.Build()
	if args.err != nil {
		return false, fmt.Errorf("register response message build failed: %v", args.err)
	}
	// 实现对全部消息的加密
	if args.frame.Type.EncryptionAllowed() {
		args.frame.Data, args.err = e.Crypto().Encrypt(args.frame.Data)
	}

	return true, args.err
}

func (e *Engine) defaultHookHandler(frame *proto.TransferFrame, con transfer.Conn) (bool, error) {
	args := e.ePool.getArgs(frame, con)

	return e.flowToHandler(args, frame.Type)
}

// 分发消息
func (e *Engine) distribute(frame *proto.TransferFrame, con transfer.Conn) {
	var err error
	var needResp bool

	if proto.GetDescriptor(frame.Type).MessageType() != proto.NotImplementMessageType {
		// 协议已实现
		needResp, err = e.hooks[frame.Type].Handler(frame, con)
	} else {
		// 此协议未注册, 通过事件回调处理
		needResp, err = e.EventHandler().OnNotImplementMessageType(frame, con)
	}

	// 业务处理错误或不需要回写返回值
	if err != nil || !needResp {
		e.Logger().Warn(fmt.Sprintf(
			"message: %s processing complete, response: %t, err: %v",
			proto.GetDescriptor(frame.Type).Text(), needResp, err,
		))
		return
	}

	// 重新构建并写入消息帧
	_, _ = con.Write(frame.Build())
	err = con.Drain()
	if err != nil {
		e.Logger().Warn(fmt.Sprintf(
			"send <message:%d> to '%s' failed: %s",
			frame.Type, con.Addr(), err,
		))
	}
}

// 断开与客户端的连接
func (e *Engine) closeConnection(addr string) {
	err := e.transfer.Close(addr)
	if err != nil {
		e.Logger().Warn("failed to disconnect with: ", err.Error())
	}
}

type EPool struct {
	args  *sync.Pool // *ChainArgs
	mResp *sync.Pool // *proto.MessageResponse
}

// 获取一个 ChainArgs 已初始化 ChainArgs.frame, ChainArgs.con, ChainArgs.resp
func (e *EPool) getArgs(frame *proto.TransferFrame, con transfer.Conn) *ChainArgs {
	args := e.args.Get().(*ChainArgs)
	args.frame = frame
	args.con = con

	// MessageResponse
	resp := e.mResp.Get().(*proto.MessageResponse)
	resp.Status = proto.RefusedStatus
	resp.Offset = 0
	resp.ReceiveTime = time.Now().Unix()

	args.resp = resp

	return args
}

func (e *EPool) putArgs(args *ChainArgs) {
	args.resp.Reset()
	e.mResp.Put(args.resp)

	args.Reset()
	e.args.Put(args)
}
