package engine

import (
	"github.com/Chendemo12/functools/tcp"
	"github.com/Chendemo12/micromq/src/proto"
	"sync"
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
	index int
	mu    *sync.Mutex
	Addr  string          `json:"addr"`
	Conf  *ConsumerConfig `json:"conf"`
	Conn  *tcp.Remote     `json:"-"`
}

func (c *Consumer) reset() *Consumer {
	c.Conn = nil
	c.Addr = ""
	c.Conf = nil

	return c
}

func (c *Consumer) setConn(r *tcp.Remote) *Consumer {
	c.Conn = r

	return c
}

// NeedConfirm 是否需要返回确认消息给客户端
func (c *Consumer) NeedConfirm() bool { return c.Conf.Ack != proto.NoConfirm }

// Send 向消费者客户端推送消息, 此操作是线程安全的
func (c *Consumer) Send(msg proto.Message) error {
	frame := framePool.Get()
	defer framePool.Put(frame)

	_bytes, err := frame.BuildFrom(msg)
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	_, err = c.Conn.Write(_bytes)
	if err != nil {
		return err
	}
	return c.Conn.Drain()
}

type Producer struct {
	index int
	mu    *sync.Mutex
	Addr  string          `json:"addr"`
	Conf  *ProducerConfig `json:"conf"`
	Conn  *tcp.Remote     `json:"-"`
}

func (p *Producer) reset() *Producer {
	p.Addr = ""
	p.Conn = nil
	p.Conf = nil

	return p
}

func (p *Producer) NeedConfirm() bool { return p.Conf.Ack != proto.NoConfirm }
