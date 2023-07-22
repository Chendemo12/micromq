package sdk

import (
	"context"
	"github.com/Chendemo12/functools/logger"
	"github.com/Chendemo12/functools/tcp"
	"github.com/Chendemo12/synshare-mq/src/proto"
)

// Config 生产者和消费者配置参数
type Config struct {
	Host   string          `json:"host"`
	Port   string          `json:"port"`
	Ack    proto.AckType   `json:"ack"`
	Ctx    context.Context `json:"-"` // 作用于生产者的父context，默认为 context.Background()
	Logger logger.Iface    `json:"-"`
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

	return c
}

type Link struct {
	Host    string          `json:"host"`
	Port    string          `json:"port"`
	Kind    proto.LinkType  `json:"kind"`
	client  *tcp.Client     // 对端连接
	handler tcp.HandlerFunc // 消息处理程序
	logger  logger.Iface
}

func (l *Link) IsConsumer() bool { return l.Kind == proto.ConsumerLinkType }
func (l *Link) IsProducer() bool { return l.Kind == proto.ProducerLinkType }

// Connect 阻塞式连接
func (l *Link) Connect() error {
	c := tcp.NewTcpClient(&tcp.TcpcConfig{
		Logger:         l.logger,
		MessageHandler: l.handler,
		Host:           l.Host,
		Port:           l.Port,
		ByteOrder:      "big",
		ReconnectDelay: 2,
		Reconnect:      true,
	})

	l.client = c
	return c.Start()
}
