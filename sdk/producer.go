package sdk

import (
	"context"
	"errors"
	"fmt"
	"github.com/Chendemo12/fastapi-tool/helper"
	"github.com/Chendemo12/fastapi-tool/logger"
	"github.com/Chendemo12/functools/tcp"
	"github.com/Chendemo12/micromq/src/proto"
	"github.com/Chendemo12/micromq/src/transfer"
	"io"
	"strings"
	"sync/atomic"
	"time"
)

type ProducerHandler interface {
	OnClose()                                            // 阻塞调用
	OnRegistered()                                       // 阻塞调用
	OnRegisterFailed(status proto.MessageResponseStatus) // 阻塞调用, 当注册失败触发的事件
	OnRegisterExpire()                                   // 阻塞调用
	// OnNotImplementMessageType 当收到一个未实现的消息帧时触发的事件
	OnNotImplementMessageType(frame *proto.TransferFrame, r transfer.Conn)
}

// PHandler 默认实现
type PHandler struct {
	logger logger.Iface
}

func (h PHandler) OnClose() {}

func (h PHandler) OnRegistered() {
	h.logger.Info("producer register successfully")
}

func (h PHandler) OnRegisterExpire() {}

func (h PHandler) OnRegisterFailed(status proto.MessageResponseStatus) {}

func (h PHandler) OnNotImplementMessageType(frame *proto.TransferFrame, r transfer.Conn) {}

// Producer 生产者, 通过 Send 发送的消息并非会立即投递给服务端
// 而是会按照服务器下发的配置定时批量发送消息,通常为500ms
type Producer struct {
	conf          *Config      //
	link          Link         // 底层数据连接
	isConnected   *atomic.Bool // tcp是否连接成功
	isRegister    *atomic.Bool // 是否注册成功
	regFrameBytes []byte       // 注册消息帧字节流
	queue         chan *proto.ProducerMessage
	tickInterval  time.Duration
	ctx           context.Context
	cancel        context.CancelFunc
	handler       ProducerHandler
	dingDong      chan struct{}
	ackTime       time.Time
}

// 收到来自服务端的消息发送成功确认消息
func (p *Producer) receiveFin() {
	p.ackTime = time.Now()
	if p.conf.Ack == proto.NoConfirm {
		// TODO: ack 未实现
	}
}

// 每滴答一次，就产生一个数据发送信号
func (p *Producer) tick() {
	for {
		select {
		case <-p.Done():
			return
		default:
			// 此操作以支持实时修改发送周期
			time.Sleep(p.tickInterval)
			p.dingDong <- struct{}{} // 发送信号
		}
	}
}

func (p *Producer) sendToServer() {
	var rate byte = 2
	for {
		if !p.CanPublisher() { // 客户端未连接或注册失败
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
			p.Stop()
			return

		case <-p.dingDong:

		// TODO: 定时批量发送消息
		case pm := <-p.queue:
			serverPM := &proto.PMessage{
				Topic: helper.S2B(pm.Topic),
				Key:   helper.S2B(pm.Key),
				Value: pm.Value,
			}

			frame := framePool.Get()
			_bytes, err := frame.BuildFrom(serverPM)

			// release
			framePool.Put(frame)
			hmPool.PutPM(pm)

			if err != nil { // 可能性很小
				continue
			}

			go func() { // 异步发送消息
				_, err2 := p.link.Write(_bytes)
				err2 = p.link.Drain()
				if err2 != nil {
					p.Logger().Warn("send message to server failed: ", err2)
				}
			}()
		}
	}
}

// 修改滴答周期, 此周期由服务端修改
func (p *Producer) modifyTick(interval time.Duration) {
	if interval < time.Second*10 && interval > time.Millisecond*100 {
		p.tickInterval = interval
	}
}

func (p *Producer) handleRegisterMessage(frame *proto.TransferFrame, r transfer.Conn) {
	form := &proto.MessageResponse{}
	err := frame.Unmarshal(form)
	if err != nil {
		p.Logger().Warn("register message response unmarshal failed: ", err.Error())
		_ = p.ReRegister(r) // retry
		return
	}

	// 处理注册响应, 目前由服务器保证重新注册等流程
	if form.Status == proto.AcceptedStatus {
		p.Logger().Info("producer register successfully")
	} else {
		p.Logger().Warn("producer register failed: ", proto.GetMessageResponseStatusText(form.Status))
	}

	switch form.Status {
	case proto.AcceptedStatus:
		p.isRegister.Store(true)
		p.handler.OnRegistered()
	case proto.RefusedStatus:
		p.isRegister.Store(false)
		p.handler.OnRegisterFailed(proto.RefusedStatus)
	case proto.TokenIncorrectStatus:
		p.isRegister.Store(false)
		p.handler.OnRegisterFailed(proto.TokenIncorrectStatus)
	}
}

// IsConnected 与服务端是否连接成功
func (p *Producer) IsConnected() bool { return p.isConnected.Load() }

// IsRegistered 向服务端注册消费者是否成功
func (p *Producer) IsRegistered() bool { return p.isRegister.Load() }

// CanPublisher 是否可以向服务器发送消息
func (p *Producer) CanPublisher() bool {
	return p.isConnected.Load() && p.isRegister.Load()
}

func (p *Producer) Done() <-chan struct{} { return p.ctx.Done() }

func (p *Producer) Logger() logger.Iface { return p.conf.Logger }

// ReRegister 服务器令客户端重新发起注册流程
func (p *Producer) ReRegister(r transfer.Conn) error {
	p.isRegister.Store(false)
	_, _ = r.Write(p.regFrameBytes)
	return r.Drain()
}

func (p *Producer) distribute(frame *proto.TransferFrame, r transfer.Conn) {
	switch frame.Type {
	case proto.RegisterMessageRespType:
		p.handleRegisterMessage(frame, r)

	case proto.ReRegisterMessageType:
		p.isRegister.Store(false)
		p.Logger().Warn("producer register expire, sever let re-register.")
		p.handler.OnRegisterExpire()
		_ = p.ReRegister(r)

	case proto.MessageRespType:
		p.receiveFin()

	default: // 未识别的帧类型
		p.handler.OnNotImplementMessageType(frame, r)
	}
}

// NewRecord 从池中初始化一个新的消息记录
func (p *Producer) NewRecord() *proto.ProducerMessage {
	return hmPool.GetPM()
}

// PutRecord 主动归还消息记录到池，仅在主动调用 NewRecord 却没发送数据时使用
func (p *Producer) PutRecord(msg *proto.ProducerMessage) {
	hmPool.PutPM(msg)
}

// Publisher 发送消息
func (p *Producer) Publisher(msg *proto.ProducerMessage) error {
	if msg.Topic == "" {
		p.PutRecord(msg)
		return ErrTopicEmpty
	}

	if !p.IsConnected() {
		p.PutRecord(msg)
		return ErrProducerUnconnected
	}
	// 未注册成功，禁止发送消息
	if !p.IsRegistered() {
		p.PutRecord(msg)
		return ErrProducerUnregistered
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

// ================================ TCP handler ===============================

func (p *Producer) OnAccepted(r *tcp.Remote) error {
	p.Logger().Info("producer connected, send register message...")

	p.isConnected.Store(true)
	return p.ReRegister(r)
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
	err := frame.ParseFrom(r) // 此操作不应并发读取，避免消息2覆盖消息1的缓冲区

	// 异步执行，立刻读取下一条消息
	go func(f *proto.TransferFrame, client transfer.Conn, err error) { // 处理消息帧
		defer framePool.Put(f)

		if err != nil {
			if !errors.Is(err, io.EOF) {
				p.Logger().Warn(fmt.Errorf("producer parse frame failed: %v", err))
			}
		} else {
			p.distribute(f, client)
		}
	}(frame, r, err)

	return nil
}

// ================================ UDP handler ===============================
//

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
	p.isRegister.Store(false)
	p.isConnected.Store(false)
	p.cancel()
	_ = p.link.Close()
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

// NewProducer 创建异步生产者,需手动启动
func NewProducer(conf Config, handlers ...ProducerHandler) *Producer {
	c := &Config{
		Host:   conf.Host,
		Port:   conf.Port,
		Ack:    conf.Ack,
		Ctx:    conf.Ctx,
		Logger: conf.Logger,
		Token:  proto.CalcSHA(conf.Token),
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

	switch strings.ToUpper(c.Link) {
	case "UDP": // TODO: 目前仅支持TCP

	default:
		p.link = &TCPLink{
			Host:   c.Host,
			Port:   c.Port,
			Kind:   proto.ProducerLinkType,
			logger: c.Logger,
		}
		p.link.SetTCPHandler(p)
	}

	if len(handlers) > 0 && handlers[0] != nil {
		p.handler = handlers[0]
	} else {
		p.handler = &PHandler{logger: conf.Logger}
	}

	frame := framePool.Get()
	p.regFrameBytes, _ = frame.BuildFrom(&proto.RegisterMessage{
		Topics: []string{},
		Ack:    AllConfirm,
		Type:   proto.ProducerLinkType,
		Token:  p.conf.Token,
	})
	framePool.Put(frame)

	return p
}

// NewAsyncProducer 创建异步生产者,无需再手动启动
func NewAsyncProducer(conf Config, handlers ...ProducerHandler) (*Producer, error) {
	p := NewProducer(conf, handlers...)

	return p, p.Start()
}
