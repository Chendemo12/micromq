package sdk

import (
	"context"
	"github.com/Chendemo12/fastapi-tool/logger"
	"github.com/Chendemo12/functools/tcp"
	"github.com/Chendemo12/micromq/src/proto"
)

// Config 生产者和消费者配置参数
type Config struct {
	Host   string          `json:"host"`
	Port   string          `json:"port"`
	Ack    proto.AckType   `json:"ack"`
	Link   string          `json:"link" description:"tcp/udp"`
	Ctx    context.Context `json:"-"` // 作用于生产者的父context，默认为 context.Background()
	Logger logger.Iface    `json:"-"`
	Token  string          `json:"-"`
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
	if c.Link == "" {
		c.Link = "tcp"
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

type TCPLink struct {
	Host    string          `json:"host"`
	Port    string          `json:"port"`
	Kind    proto.LinkType  `json:"kind"`
	client  *tcp.Client     // 对端连接
	handler tcp.HandlerFunc // 消息处理程序
	logger  logger.Iface
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
