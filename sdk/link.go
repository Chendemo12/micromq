package sdk

import (
	"github.com/Chendemo12/functools/logger"
	"github.com/Chendemo12/functools/tcp"
	"github.com/Chendemo12/synshare-mq/src/proto"
	"sync"
)

type Config struct {
	Host   string `json:"host"`
	Port   string `json:"port"`
	Ack    proto.AckType
	Logger logger.Iface
}

type Link struct {
	kind    proto.LinkType
	conf    *Config         // 配置参数
	client  *tcp.Client     // 对端连接
	handler tcp.HandlerFunc // 消息处理程序
	mu      *sync.Mutex
}

func (l *Link) IsConsumer() bool { return l.kind == proto.ConsumerLinkType }
func (l *Link) IsProducer() bool { return l.kind == proto.ProducerLinkType }

// Connect 阻塞式连接
func (l *Link) Connect() error {
	c := tcp.NewTcpClient(&tcp.TcpcConfig{
		Logger:         nil,
		MessageHandler: l.handler,
		Host:           l.conf.Host,
		Port:           l.conf.Port,
		ByteOrder:      "big",
		ReconnectDelay: 2,
		Reconnect:      true,
	})

	l.client = c
	return c.Start()
}
