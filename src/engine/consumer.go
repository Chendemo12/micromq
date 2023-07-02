package engine

import (
	"github.com/Chendemo12/functools/tcp"
)

type ConsumerConfig struct {
	Topics []string `json:"topics"`
	Ack    AckType  `json:"ack"`
}

type Consumer struct {
	Conf *ConsumerConfig `json:"conf"`
	Addr string          `json:"addr"`
	Conn *tcp.Remote     `json:"-"`
}

func (c *Consumer) setConn(r *tcp.Remote) {
	c.Conn = r
}

// NeedConfirm 是否需要返回确认消息给客户端
func (c *Consumer) NeedConfirm() bool { return c.Conf.Ack != NoConfirm }
