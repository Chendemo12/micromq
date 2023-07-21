package sdk

import (
	"context"
	"fmt"
	"github.com/Chendemo12/fastapi-tool/helper"
	"github.com/Chendemo12/functools/logger"
	"github.com/Chendemo12/functools/tcp"
	"github.com/Chendemo12/synshare-mq/src/proto"
	"sync"
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
	link          *Link // 底层数据连接
	isConnected   *atomic.Bool
	isRegister    *atomic.Bool // 是否注册成功
	regFrameBytes []byte       // 注册消息帧
	queue         chan *proto.ProducerMessage
	sendInterval  time.Duration
	ticker        *time.Ticker
	logger        logger.Iface
	ctx           context.Context
	cancel        context.CancelFunc
	handler       ProducerHandler
}

func (p *Producer) OnAccepted(r *tcp.Remote) error {
	p.logger.Info("producer connected, send register message...")

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
	p.logger.Info("producer connection lost, reconnect...")

	p.isConnected.Store(false)
	p.isRegister.Store(false)
	p.handler.OnClose()

	return nil
}

// 收到来自服务端的消息发送成功确认消息
func (p *Producer) receiveFin() {
	// TODO: ack 未实现
	if p.link.conf.Ack == proto.NoConfirm {
	}
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
		p.logger.Debug("sever let re-register")
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
	for {
		if !p.isConnected.Load() { // 客户端未连接
			time.Sleep(p.sendInterval * 2)
			continue
		}

		select {
		case <-p.ctx.Done():
			return

		case <-p.ticker.C:

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
					p.logger.Warn("send message to server failed: ", err2)
				}
			}()
		}
	}
}

func (p *Producer) Start() error {
	p.ticker = time.NewTicker(time.Millisecond * 500) // 500 ms
	p.ctx, p.cancel = context.WithCancel(context.Background())

	err := p.link.Connect()
	if err != nil {
		// 连接服务器失败
		return err
	}

	go p.sendToServer()

	return nil
}

func (p *Producer) Stop() {
	p.cancel()
	_ = p.link.client.Stop()
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
	if conf.Logger == nil {
		conf.Logger = logger.NewDefaultLogger()
	}
	p := &Producer{
		isConnected:  &atomic.Bool{},
		isRegister:   &atomic.Bool{},
		queue:        make(chan *proto.ProducerMessage, 10),
		logger:       conf.Logger,
		sendInterval: DefaultProducerSendInterval,
	}
	p.link = &Link{
		kind:    proto.ProducerLinkType,
		conf:    &Config{Host: conf.Host, Port: conf.Port},
		mu:      &sync.Mutex{},
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
