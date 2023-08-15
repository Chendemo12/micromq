package sdk

import (
	"context"
	"errors"
	"fmt"
	"github.com/Chendemo12/fastapi-tool/logger"
	"github.com/Chendemo12/functools/tcp"
	"github.com/Chendemo12/micromq/src/proto"
	"github.com/Chendemo12/micromq/src/transfer"
	"io"
	"strings"
	"sync/atomic"
	"time"
)

// Config 生产者和消费者配置参数
type Config struct {
	Host     string          `json:"host"`
	Port     string          `json:"port"`
	Ack      proto.AckType   `json:"ack"`
	LinkType string          `json:"link_type" description:"tcp/udp"`
	PCtx     context.Context `json:"-"` // 父context，默认为 context.Background()
	Logger   logger.Iface    `json:"-"`
	Token    string          `json:"-"`
}

func (c *Config) Clean() *Config {
	if c.Port == "" {
		c.Port = "7270"
	}
	if c.Logger == nil {
		c.Logger = logger.NewDefaultLogger()
	}
	if c.PCtx == nil {
		c.PCtx = context.Background()
	}
	if c.Ack == "" {
		c.Ack = proto.AllConfirm
	}
	if c.LinkType == "" {
		c.LinkType = "tcp"
	}

	return c
}

type Link interface {
	Connect() error
	Close() error
	SetTCPHandler(handler tcp.HandlerFunc)
	SetUDPHandler(handler func())
	Write(p []byte) (int, error) // 将切片buf中的内容追加到发数据缓冲区内，并返回写入的数据长度
	Drain() error                // 将缓冲区的数据发生到客户端
}

// Broker Broker连接管理，负责连接服务器并完成注册任务
// 在检测到连接断开时主动重连
type Broker struct {
	conf          *Config
	ctx           context.Context
	cancel        context.CancelFunc
	isConnected   *atomic.Bool           // tcp是否连接成功
	isRegister    *atomic.Bool           // 是否注册成功
	resp          *proto.MessageResponse // 注册响应
	regFrameBytes []byte                 // 注册消息帧字节流
	link          Link                   // 底层数据连接
	linkType      proto.LinkType         // 客户端连接
	event         ProducerHandler        // 事件触发器
	// 消息处理器
	messageHandler func(frame *proto.TransferFrame, con transfer.Conn)
}

func (b *Broker) handleRegisterMessage(frame *proto.TransferFrame, con transfer.Conn) {
	err := frame.Unmarshal(b.resp)
	if err != nil {
		b.Logger().Warn("register message response unmarshal failed: ", err.Error())
		_ = b.ReRegister() // retry
		return
	}

	// 处理注册响应, 目前由服务器保证重新注册等流程
	if b.resp.Status == proto.AcceptedStatus {
		b.Logger().Info(b.linkType + " register successfully")
		b.isRegister.Store(true)
		b.event.OnRegistered()
	} else {
		b.Logger().Warn(b.linkType+" register failed: ", proto.GetMessageResponseStatusText(b.resp.Status))
		b.isRegister.Store(false)
		b.event.OnRegisterFailed(b.resp.Status)
	}
}

func (b *Broker) distribute(frame *proto.TransferFrame, con transfer.Conn) {
	switch frame.Type {

	case proto.RegisterMessageRespType: // 处理注册响应
		b.handleRegisterMessage(frame, con)

	case proto.ReRegisterMessageType: // 服务器令客户端重新注册
		b.isRegister.Store(false)
		b.Logger().Debug(b.linkType, " register expire, sever let re-register")
		b.event.OnRegisterExpire()
		_ = b.ReRegister()

	default: // 处理其他消息类型，交由上层处理
		b.messageHandler(frame, con)
	}
}

func (b *Broker) init() *Broker {
	b.ctx, b.cancel = context.WithCancel(b.conf.PCtx)

	b.resp = &proto.MessageResponse{}
	b.isRegister = &atomic.Bool{}
	b.isConnected = &atomic.Bool{}

	return b
}

// ================================ interface ===============================

// IsConnected 与服务端是否连接成功
func (b *Broker) IsConnected() bool { return b.isConnected.Load() }

// IsRegistered 向服务端注册消费者是否成功
func (b *Broker) IsRegistered() bool { return b.isRegister.Load() }

// StatusOK 连接状态是否正常
func (b *Broker) StatusOK() bool { return b.isConnected.Load() && b.isRegister.Load() }

func (b *Broker) Logger() logger.Iface { return b.conf.Logger }

func (b *Broker) LinkType() proto.LinkType { return b.linkType }

func (b *Broker) Done() <-chan struct{} { return b.ctx.Done() }

func (b *Broker) HeartbeatTask() {
	for {
		select {
		case <-b.ctx.Done():
			return
		default:
			// 此操作以支持实时修改发送周期
			time.Sleep(b.HeartbeatInterval())
			if !b.StatusOK() {
				continue
			}
			// 发送心跳
			m := &proto.HeartbeatMessage{
				Type: b.linkType, CreatedAt: time.Now().Unix(),
			}
			frame := framePool.Get()
			_bytes, err := frame.BuildFrom(m)
			// release
			framePool.Put(frame)

			if err != nil {
				continue
			}
			_, _ = b.link.Write(_bytes)
			_ = b.link.Drain()
		}
	}
}

// ReRegister 重新发起注册流程
func (b *Broker) ReRegister() error {
	_, _ = b.link.Write(b.regFrameBytes)

	return b.link.Drain()
}

// TickerInterval 数据发送周期
func (b *Broker) TickerInterval() time.Duration {
	if b.resp.TickerInterval == 0 {
		return DefaultProducerSendInterval
	}
	return time.Duration(b.resp.TickerInterval) * time.Millisecond
}

// HeartbeatInterval 心跳周期
func (b *Broker) HeartbeatInterval() time.Duration {
	if b.resp.Keepalive == 0 {
		return DefaultProducerSendInterval * 30 // 15s
	}
	return time.Duration(b.resp.Keepalive) * time.Second
}

func (b *Broker) SetRegisterMessage(message *proto.RegisterMessage) error {
	frame := framePool.Get()
	message.Type = b.linkType
	message.Token = b.conf.Token
	message.Ack = b.conf.Ack

	_bytes, err := frame.BuildFrom(message)
	framePool.Put(frame)

	if err != nil {
		return err
	}

	b.regFrameBytes = _bytes

	return nil
}

func (b *Broker) SetTransfer(trans string) *Broker {
	if strings.ToUpper(trans) == "UDP" {

	} else { // TCP
		b.link = &TCPLink{
			Host:     b.conf.Host,
			Port:     b.conf.Port,
			LinkType: b.linkType,
			handler:  b,
			logger:   b.Logger(),
		}
		b.link.SetTCPHandler(b)
	}

	return b
}

// ================================ TCP messageHandler ===============================

// OnAccepted 当TCP连接成功时会自行发送注册消息
func (b *Broker) OnAccepted(_ *tcp.Remote) error {
	b.Logger().Debug(b.linkType + " connected, send register message...")

	b.isConnected.Store(true)
	b.isRegister.Store(false)
	b.event.OnConnected()

	// 连接成功,发送注册消息
	return b.ReRegister()
}

func (b *Broker) OnClosed(_ *tcp.Remote) error {
	b.Logger().Warn(b.linkType + " connection lost, reconnect...")

	b.isConnected.Store(false)
	b.isRegister.Store(false)
	b.event.OnClosed()

	return nil
}

func (b *Broker) Handler(r *tcp.Remote) error {
	if r.Len() < proto.FrameMinLength {
		//return proto.ErrMessageNotFull
		return nil
	}

	frame := framePool.Get()
	err := frame.ParseFrom(r) // 此操作不应并发读取，避免消息2覆盖消息1的缓冲区

	// 异步执行，立刻读取下一条消息
	go func(f *proto.TransferFrame, client transfer.Conn, err error) { // 处理消息帧
		defer framePool.Put(f)

		if err != nil {
			if !errors.Is(err, io.EOF) {
				b.Logger().Warn(fmt.Errorf("%s parse frame failed: %v", b.linkType, err))
			}
		} else {
			b.distribute(f, client)
		}
	}(frame, r, err)

	return nil
}
