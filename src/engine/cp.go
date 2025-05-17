package engine

import (
	"sync"
	"time"

	"github.com/Chendemo12/micromq/src/proto"
	"github.com/Chendemo12/micromq/src/transfer"
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
	Conn  transfer.Conn   `json:"-"`
}

func (c *Consumer) reset() *Consumer {
	c.Addr = ""
	c.Conf = nil
	c.Conn = nil

	return c
}

func (c *Consumer) IsFree() bool { return c.Addr == "" }

func (c *Consumer) SetConn(r transfer.Conn) *Consumer {
	c.Addr = r.Addr()
	c.Conn = r

	return c
}

func (c *Consumer) Index() int { return c.index }

// NeedConfirm 是否需要返回确认消息给客户端
func (c *Consumer) NeedConfirm() bool { return c.Conf.Ack != proto.NoConfirm }

// Send 向消费者客户端推送消息, 此操作是线程安全的
func (c *Consumer) Send(msg proto.Message) error {
	frame := framePool.Get()
	defer framePool.Put(frame)

	err := frame.BuildFrom(msg)
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	_, err = frame.WriteTo(c.Conn)
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
	Conn  transfer.Conn   `json:"-"`
}

func (p *Producer) reset() *Producer {
	p.Addr = ""
	p.Conf = nil
	p.Conn = nil

	return p
}

func (p *Producer) IsFree() bool { return p.Addr == "" }

func (p *Producer) SetConn(r transfer.Conn) *Producer {
	p.Addr = r.Addr()
	p.Conn = r

	return p
}

func (p *Producer) Index() int { return p.index }

func (p *Producer) NeedConfirm() bool { return p.Conf.Ack != proto.NoConfirm }
