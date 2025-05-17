package sdk

import (
	"github.com/Chendemo12/functools/logger"
	"github.com/Chendemo12/functools/tcp"
	"github.com/Chendemo12/micromq/src/proto"
)

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
