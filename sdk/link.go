package sdk

import (
	"github.com/Chendemo12/functools/tcp"
	"github.com/Chendemo12/synshare-mq/src/proto"
	"sync"
	"time"
)

type Config struct {
	Host string `json:"host"`
	Port string `json:"port"`
	Ack  proto.AckType
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
	var err error
	go func() {
		err = c.Start()
	}()
	time.Sleep(time.Millisecond * 200)

	return err
}
