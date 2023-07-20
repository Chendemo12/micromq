package engine

import (
	"github.com/Chendemo12/functools/tcp"
	"github.com/Chendemo12/synshare-mq/src/proto"
	"time"
)

type ConsumerConfig struct {
	Topics []string      `json:"topics"`
	Ack    proto.AckType `json:"ack"`
}

type ProducerConfig struct {
	Ack proto.AckType `json:"ack"`
	// 定时器间隔，单位ms，仅生产者有效，生产者需要按照此间隔发送帧消息
	TickerInterval time.Duration `json:"ticker_duration"`
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
func (c *Consumer) NeedConfirm() bool { return c.Conf.Ack != proto.NoConfirm }

type Producer struct {
	Conf *ProducerConfig `json:"conf"`
	Addr string          `json:"addr"`
	Conn *tcp.Remote     `json:"-"`
}
