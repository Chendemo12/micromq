package sdk

import (
	"github.com/Chendemo12/functools/tcp"
	"sync"
)

type LinkKind string

const (
	consumerLinkKind LinkKind = "consumer"
	producerLinkKind LinkKind = "producer"
)

type Config struct {
	Host string `json:"host"`
	Port string `json:"port"`
}

type Link struct {
	kind        LinkKind
	conf        *Config         // 配置参数
	conn        *tcp.Remote     // 对端连接
	handler     tcp.HandlerFunc // 消息处理程序
	isConnected bool
	isRegister  bool
	mu          *sync.Mutex
}

func (l *Link) IsConsumer() bool { return l.kind == consumerLinkKind }
