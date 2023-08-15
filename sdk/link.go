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
	"sync/atomic"
	"time"
)

// Config 生产者和消费者配置参数
type Config struct {
	Host     string          `json:"host"`
	Port     string          `json:"port"`
	Ack      proto.AckType   `json:"ack"`
	LinkType string          `json:"link_type" description:"tcp/udp"`
	Ctx      context.Context `json:"-"` // 作用于生产者的父context，默认为 context.Background()
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
	if c.Ctx == nil {
		c.Ctx = context.Background()
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

type Broker struct {
	conf          *Config                //
	isConnected   *atomic.Bool           // tcp是否连接成功
	isRegister    *atomic.Bool           // 是否注册成功
	resp          *proto.MessageResponse // 注册响应
	regFrameBytes []byte                 // 注册消息帧字节流
	link          Link                   // 底层数据连接
	linkType      proto.LinkType
	distribute    func(frame *proto.TransferFrame, r transfer.Conn)
	handler       ProducerHandler
}

// IsConnected 与服务端是否连接成功
func (p *Broker) IsConnected() bool { return p.isConnected.Load() }

// IsRegistered 向服务端注册消费者是否成功
func (p *Broker) IsRegistered() bool { return p.isRegister.Load() }

// StatusOK 连接状态是否正常
func (p *Broker) StatusOK() bool { return p.isConnected.Load() && p.isRegister.Load() }

func (p *Broker) Logger() logger.Iface { return p.conf.Logger }

func (p *Broker) LinkType() proto.LinkType { return p.linkType }

func (p *Broker) Done() <-chan struct{} { return p.conf.Ctx.Done() }

// ReRegister 服务器令客户端重新发起注册流程
func (p *Broker) ReRegister(r transfer.Conn) error {
	p.isRegister.Store(false)
	_, _ = r.Write(p.regFrameBytes)
	return r.Drain()
}

// TickerInterval 数据发送周期
func (p *Broker) TickerInterval() time.Duration {
	if p.resp.TickerInterval == 0 {
		return DefaultProducerSendInterval
	}
	return time.Duration(p.resp.TickerInterval) * time.Millisecond
}

// HeartbeatInterval 心跳周期
func (p *Broker) HeartbeatInterval() time.Duration {
	if p.resp.Keepalive == 0 {
		return DefaultProducerSendInterval * 30 // 15s
	}
	return time.Duration(p.resp.Keepalive) * time.Second
}

func (p *Broker) OnAccepted(r *tcp.Remote) error {
	p.Logger().Debug(p.linkType + " connected, send register message...")
	p.resp = &proto.MessageResponse{}
	p.isConnected.Store(true)
	p.handler.OnConnected()
	return p.ReRegister(r)
}

func (p *Broker) OnClosed(_ *tcp.Remote) error {
	p.Logger().Warn(p.linkType + " connection lost, reconnect...")

	p.isConnected.Store(false)
	p.isRegister.Store(false)
	p.handler.OnClosed()

	return nil
}

func (p *Broker) Handler(r *tcp.Remote) error {
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
				p.Logger().Warn(fmt.Errorf("%s parse frame failed: %v", p.linkType, err))
			}
		} else {
			p.distribute(f, client)
		}
	}(frame, r, err)

	return nil
}

// CanPublisher 是否可以向服务器发送消息
func (p *Broker) CanPublisher() bool {
	return p.isConnected.Load() && p.isRegister.Load()
}

func (p *Broker) heartbeat() {
	for {
		select {
		case <-p.Done():
			return
		default:
			// 此操作以支持实时修改发送周期
			time.Sleep(p.HeartbeatInterval())
			if !p.IsRegistered() {
				continue
			}
			// 发送心跳
			m := &proto.HeartbeatMessage{
				Type:      proto.ProducerLinkType,
				CreatedAt: time.Now().Unix(),
			}
			frame := framePool.Get()
			_bytes, err := frame.BuildFrom(m)
			// release
			framePool.Put(frame)
			if err != nil {
				continue
			}
			_, _ = p.link.Write(_bytes)
			_ = p.link.Drain()
		}
	}
}

type TCPLink struct {
	Host     string          `json:"host"`
	Port     string          `json:"port"`
	LinkType proto.LinkType  `json:"link_type"`
	client   *tcp.Client     // 对端连接
	handler  tcp.HandlerFunc // 消息处理程序
	logger   logger.Iface
}

func (l *TCPLink) Write(p []byte) (int, error) { return l.client.Write(p) }

func (l *TCPLink) Drain() error { return l.client.Drain() }

func (l *TCPLink) SetTCPHandler(handler tcp.HandlerFunc) {
	l.handler = handler
}

func (l *TCPLink) SetUDPHandler(_ func()) {}

func (l *TCPLink) Close() error {
	if l.client != nil {
		return l.client.Stop()
	}
	return nil
}

// Connect 阻塞式连接
func (l *TCPLink) Connect() error {
	c := tcp.NewTcpClient(&tcp.TcpcConfig{
		Logger:         l.logger,
		MessageHandler: l.handler,
		Host:           l.Host,
		Port:           l.Port,
		ByteOrder:      "big",
		ReconnectDelay: 2, // 此参数单位为s
		Reconnect:      true,
	})

	l.client = c
	return c.Start()
}
