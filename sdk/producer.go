package sdk

import (
	"context"
	"fmt"
	"github.com/Chendemo12/fastapi-tool/helper"
	"github.com/Chendemo12/functools/logger"
	"github.com/Chendemo12/functools/tcp"
	"github.com/Chendemo12/synshare-mq/src/proto"
	"sync/atomic"
	"time"
)

type ProducerHandler interface {
	OnRegistered()     // 阻塞调用
	OnClose()          // 阻塞调用
	OnRegisterExpire() // 阻塞调用
}

// Producer 生产者, 通过 Send 发送的消息并非会立即投递给服务端
// 而是会按照服务器下发的配置定时批量发送消息,通常为500ms
type Producer struct {
	conf          *Config      //
	link          *Link        // 底层数据连接
	isConnected   *atomic.Bool // tcp是否连接成功
	isRegister    *atomic.Bool // 是否注册成功
	regFrameBytes []byte       // 注册消息帧字节流
	queue         chan *proto.ProducerMessage
	tickInterval  time.Duration
	ctx           context.Context
	cancel        context.CancelFunc
	handler       ProducerHandler
	dingDong      chan struct{}
}

// 收到来自服务端的消息发送成功确认消息
func (p *Producer) receiveFin() {
	// TODO: ack 未实现
	if p.conf.Ack == proto.NoConfirm {
	}
}

// 每滴答一次，就产生一个数据发送信号
func (p *Producer) tick() {
	for {
		select {
		case <-p.Done():
			return
		default:
			time.Sleep(p.tickInterval)
			p.dingDong <- struct{}{} // 发送信号
		}
	}
}

// 修改滴答周期, 此周期由服务端修改
func (p *Producer) modifyTick(interval time.Duration) {
	if interval < time.Second*10 && interval > time.Millisecond*100 {
		p.tickInterval = interval
	}
}

func (p *Producer) IsConnected() bool  { return p.isConnected.Load() }
func (p *Producer) IsRegistered() bool { return p.isRegister.Load() }

func (p *Producer) Done() <-chan struct{} { return p.ctx.Done() }

func (p *Producer) Logger() logger.Iface { return p.conf.Logger }

func (p *Producer) OnAccepted(r *tcp.Remote) error {
	p.Logger().Info("producer connected, send register message...")

	p.isConnected.Store(true)
	return p.ReRegister(r)
}

// ReRegister 服务器令客户端重新发起注册流程
func (p *Producer) ReRegister(r *tcp.Remote) error {
	p.isRegister.Store(true)
	_, _ = r.Write(p.regFrameBytes)
	return r.Drain()
}

func (p *Producer) OnClosed(_ *tcp.Remote) error {
	p.Logger().Info("producer connection lost, reconnect...")

	p.isConnected.Store(false)
	p.isRegister.Store(false)
	p.handler.OnClose()

	return nil
}

func (p *Producer) Handler(r *tcp.Remote) error {
	if r.Len() < proto.FrameMinLength {
		return proto.ErrMessageNotFull
	}

	frame := framePool.Get()
	defer framePool.Put(frame)

	err := frame.ParseFrom(r)
	if err != nil {
		return fmt.Errorf("producer parse frame failed: %v", err)
	}

	switch frame.Type {
	case proto.RegisterMessageRespType:
		p.isRegister.Store(true)
		p.handler.OnRegistered()

	case proto.ReRegisterMessageType:
		p.Logger().Debug("sever let re-register")
		p.handler.OnRegisterExpire()
		return p.ReRegister(r)

	case proto.MessageRespType:
		p.receiveFin()
	}

	return nil
}

// NewRecord 从池中初始化一个新的消息记录
func (p *Producer) NewRecord() *proto.ProducerMessage {
	return mPool.GetPM()
}

// PutRecord 主动归还消息记录到池，仅在主动调用 NewRecord 却没发送数据时使用
func (p *Producer) PutRecord(msg *proto.ProducerMessage) {
	mPool.PutPM(msg)
}

// Publisher 发送消息
func (p *Producer) Publisher(msg *proto.ProducerMessage) error {
	if msg.Topic == "" {
		p.PutRecord(msg)
		return ErrTopicEmpty
	}
	p.queue <- msg
	return nil
}

// Send 发送一条消息
func (p *Producer) Send(fn func(record *proto.ProducerMessage) error) error {
	msg := p.NewRecord()
	err := fn(msg)
	if err != nil {
		p.PutRecord(msg)
		return err
	}
	return p.Publisher(msg)
}

func (p *Producer) sendToServer() {
	var rate byte = 2
	for {
		if !p.isConnected.Load() { // 客户端未连接
			if rate > 10 {
				rate = 2
			}
			time.Sleep(p.tickInterval * time.Duration(rate))
			rate++ // 等待时间逐渐延长
			continue
		}

		rate = 2 // 重置等待时间
		select {
		case <-p.ctx.Done():
			return

		case <-p.dingDong:

		// TODO: 定时批量发送消息
		case msg := <-p.queue:
			serverPM := &proto.PMessage{
				Topic: helper.S2B(msg.Topic),
				Key:   helper.S2B(msg.Key),
				Value: msg.Value,
			}

			frame := framePool.Get()
			_bytes, err := frame.BuildFrom(serverPM)

			// release
			mPool.PutPM(msg)
			framePool.Put(frame)

			if err != nil { // 可能性很小
				continue
			}

			go func() { // 异步发送消息
				_, err2 := p.link.client.Write(_bytes)
				err2 = p.link.client.Drain()
				if err2 != nil {
					p.Logger().Warn("send message to server failed: ", err2)
				}
			}()
		}
	}
}

func (p *Producer) Start() error {
	err := p.link.Connect()
	if err != nil {
		// 连接服务器失败
		return err
	}

	go p.tick()
	go p.sendToServer()

	return nil
}

func (p *Producer) Stop() {
	p.cancel()
	if p.link != nil && p.link.client != nil {
		_ = p.link.client.Stop()
	}
}

// ==================================== methods shortcut ====================================

// JSONMarshal 序列化方法
func (p *Producer) JSONMarshal(v any) ([]byte, error) {
	return helper.JsonMarshal(v)
}

func (p *Producer) JSONUnmarshal(data []byte, v any) error {
	return helper.JsonUnmarshal(data, v)
}

// Beautify 格式化显示字节流
func (p *Producer) Beautify(data []byte) string {
	return helper.HexBeautify(data)
}

type emptyPHandler struct{}

func (h emptyPHandler) OnRegistered()     {}
func (h emptyPHandler) OnClose()          {}
func (h emptyPHandler) OnRegisterExpire() {}

// NewProducer 创建异步生产者,无需再手动启动
func NewProducer(conf Config, handlers ...ProducerHandler) *Producer {
	c := &Config{
		Host:   conf.Host,
		Port:   conf.Port,
		Ack:    conf.Ack,
		Ctx:    conf.Ctx,
		Logger: conf.Logger,
	}
	p := &Producer{
		conf:         c.Clean(),
		isConnected:  &atomic.Bool{},
		isRegister:   &atomic.Bool{},
		queue:        make(chan *proto.ProducerMessage, 10),
		tickInterval: DefaultProducerSendInterval, // 此值由服务更改
	}
	p.ctx, p.cancel = context.WithCancel(p.conf.Ctx)
	p.dingDong = make(chan struct{}, 1)

	p.link = &Link{
		Host:    c.Host,
		Port:    c.Port,
		Kind:    proto.ProducerLinkType,
		handler: p,
	}

	if len(handlers) > 0 && handlers[0] != nil {
		p.handler = handlers[0]
	} else {
		p.handler = &emptyPHandler{}
	}

	frame := framePool.Get()
	p.regFrameBytes, _ = frame.BuildFrom(proto.NewPRegisterMessage())
	framePool.Put(frame)

	return p
}

// NewAsyncProducer 创建异步生产者,无需再手动启动
func NewAsyncProducer(conf Config, handlers ...ProducerHandler) (*Producer, error) {
	p := NewProducer(conf, handlers...)

	return p, p.Start()
}
