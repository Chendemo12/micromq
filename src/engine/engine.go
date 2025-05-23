package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Chendemo12/functools/cronjob"
	"github.com/Chendemo12/functools/logger"
	"github.com/Chendemo12/micromq/src/proto"
	"github.com/Chendemo12/micromq/src/transfer"
)

type Config struct {
	Host             string          `json:"host"`
	Port             string          `json:"port"`
	MaxOpenConn      int             `json:"max_open_conn"` // 允许的最大连接数, 即 生产者+消费者最多有 MaxOpenConn 个, 为0则不限制
	BufferSize       int             `json:"buffer_size"`   // 生产者消息历史记录最大数量
	HeartbeatTimeout float64         `json:"heartbeat_timeout"`
	Logger           logger.Iface    `json:"-"`
	Token            string          `json:"-"` // 注册认证密钥
	EventHandler     EventHandler    `json:"-"` // 事件触发器
	Ctx              context.Context `json:"-"`
	topicHistorySize int             // topic 历史缓存大小
}

func (c *Config) clean() *Config {
	if !(c.BufferSize > 0 && c.BufferSize <= 5000) {
		c.BufferSize = 100
	}

	if c.Logger == nil {
		c.Logger = logger.NewDefaultLogger()
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
	stat                 *Statistic
	scheduler            *cronjob.Scheduler
	ePool                *EPool                             // 池化各种数据
	tokenCrypto          *proto.TokenCrypto                 // 用于注册消息加解密
	crypto               proto.Crypto                       // 加解密器
	producerSendInterval time.Duration                      // 生产者发送消息的时间间隔 = 500ms
	hooks                [proto.TotalNumberOfMessages]*Hook // 各种协议的处理者
	// 消息帧处理链，每一个链内部无需直接向客户端写入消息,通过修改frame实现返回消息
	flows  [proto.TotalNumberOfMessages][]FlowHandler
	cpLock *sync.RWMutex // consumer producer add/remove lock
}

func (e *Engine) beforeServe() *Engine {
	// 初始化全部内存对象
	for i := 0; i < proto.TotalNumberOfMessages; i++ {
		// 初始化为未实现
		e.hooks[i] = &Hook{
			Type:       proto.NotImplementMessageType,
			Handler:    e.flowToHookHandler, // 默认实现都是这种类型
			ACKDefined: proto.GetDescriptor(proto.MessageType(i)).NeedACK(),
		}
		// 初始化一个空流程链
		e.flows[i] = make([]FlowHandler, 0)
	}

	e.producers = make([]*Producer, e.conf.MaxOpenConn)
	e.consumers = make([]*Consumer, e.conf.MaxOpenConn)

	for i := 0; i < len(e.consumers); i++ {
		e.consumers[i] = &Consumer{
			index: i,
			mu:    &sync.Mutex{},
			Conf:  &ConsumerConfig{},
			Addr:  "",
			Conn:  nil,
		}
	}
	for i := 0; i < len(e.producers); i++ {
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
	e.stat = &Statistic{broker: e}
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
	index := -1
	for i := 0; i < len(e.producers); i++ {
		// cannot be nil
		if e.producers[i].IsFree() {
			index = i
			break
		}
	}

	if index == -1 && e.conf.MaxOpenConn <= 0 {
		// 不限制连接数量，追加空间
		index = len(e.consumers)
		e.producers = append(e.producers, &Producer{
			index: index,
			mu:    &sync.Mutex{},
			Conf:  &ProducerConfig{},
			Addr:  "",
			Conn:  nil,
		})
	}

	return index
}

// 查找一个空闲的 消费者槽位，若未找到则返回 -1，应在查找之前主动加锁
func (e *Engine) findConsumerSlot() int {
	index := -1
	for i := 0; i < len(e.consumers); i++ {
		if e.consumers[i].IsFree() {
			index = i
			break
		}
	}
	if index == -1 && e.conf.MaxOpenConn <= 0 {
		// 不限制连接数量，追加空间
		index = len(e.consumers)
		e.consumers = append(e.consumers, &Consumer{
			index: index,
			mu:    &sync.Mutex{},
			Conf:  &ConsumerConfig{},
			Addr:  "",
			Conn:  nil,
		})
	}

	return index
}

// 将一系列处理过程组合成一条链
func (e *Engine) flowToHookHandler(frame *proto.TransferFrame, con transfer.Conn) error {
	args := e.ePool.getArgs(frame, con)
	defer e.ePool.putArgs(args)

	for _, link := range e.flows[frame.Type()] {
		if link(args) { // 此环节决定终止后续流程
			break
		}
	}

	// 不需要回复响应, 不再构建帧消息
	if !args.ReplyClient() {
		return ErrNoNeedToReply
	}

	// 业务处理错误, 需要返回错误响应
	if err := args.StopError(); err != nil {
		e.Logger().Warn(fmt.Sprintf(
			"<frame:%s> processing complete, but err: %v", frame.MessageText(), err,
		))
	}

	// 构建返回值
	return frame.BuildFrom(args.resp, e.Crypto().Encrypt)
}

// 分发消息
func (e *Engine) distribute(frame *proto.TransferFrame, con transfer.Conn) {
	var err error
	// 依据消息定义, 判断此消息是否应该返回响应给客户端
	var needResp = proto.GetDescriptor(frame.Type()).NeedACK()

	if e.hooks[frame.Type()].Type != proto.NotImplementMessageType {
		// 协议已实现
		err = e.hooks[frame.Type()].Handler(frame, con)
	} else {
		// 此协议未实现, 通过事件回调处理
		err = e.EventHandler().OnNotImplementMessageType(frame, con)
	}

	// 设置了不返回数据
	if err != nil && errors.Is(err, ErrNoNeedToReply) {
		needResp = false
	}

	if !needResp { // 依据定义此消息不需要有响应
		return
	}

	// 重新构建并写入消息帧
	_, err = frame.WriteTo(con)
	err = con.Drain()
	if err != nil {
		e.Logger().Warn(fmt.Sprintf(
			"send <message:%d> to '%s' failed: %s", frame.Type(), con.Addr(), err,
		))
	}
}

// 断开与客户端的连接
func (e *Engine) closeConnection(addr string) {
	if addr == "" {
		return
	}
	err := e.transfer.Close(addr)
	if err != nil {
		e.Logger().Warn("failed to disconnect with: ", err.Error())
	}
}

// DeleteTopic 关闭topic内的消费者连接，并删除topic
func (e *Engine) DeleteTopic(name string, force bool) error {
	topic, ok := e.FindTopic(name)
	if !ok { // topic不存在，直接返回成功
		return nil
	}
	consumerNum := 0
	topic.RangeConsumer(func(c *Consumer) bool {
		consumerNum++
		return false
	})
	if consumerNum > 0 && !force { // topic存在消费者，且未设置强制删除，则返回错误
		return errors.New("topic is in use")
	}

	topic.RangeConsumer(func(c *Consumer) bool {
		e.closeConnection(c.Addr) // 此处会触发 onClientClosed 回调，并从 consumers / producers 中删除连接
		return true
	})

	// 删除topic数据交换
	e.RangeTopic(func(topic *Topic) bool {
		topic.DelForwarding([]byte(name))
		return true
	})

	e.topics.Delete(name)
	return nil
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

	// 响应可以不写给客户端，但是对象必须初始化
	resp := e.mResp.Get().(*proto.MessageResponse)
	resp.Type = proto.MessageRespType
	resp.Status = proto.AcceptedStatus
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
