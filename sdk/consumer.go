package sdk

import (
	"errors"
	"github.com/Chendemo12/fastapi-tool/helper"
	"github.com/Chendemo12/functools/python"
	"github.com/Chendemo12/functools/tcp"
	"github.com/Chendemo12/synshare-mq/src/engine"
	"sync"
)

type MessageRecord = engine.Message
type ConsumerRegisterMessage = engine.RegisterMessage

type ConsumerHandler interface {
	Topics() []string
	Handler(record *MessageRecord)
	WhenConnected() // 当连接成功时,发出的信号
	WhenClosed()    // 当连接中断时,发出的信号
}

// Consumer 消费者
type Consumer struct {
	link        *Link           // 底层数据连接
	handler     ConsumerHandler // 消息处理方法
	isConnected bool
	isRegister  bool // 是否注册成功
	pool        *sync.Pool
}

func (c *Consumer) OnAccepted(r *tcp.Remote) error {
	c.isConnected = true
	// 连接成功,发送注册消息
	msg := &ConsumerRegisterMessage{
		Topics: c.handler.Topics(),
		Ack:    engine.NoConfirm,
		Type:   engine.ConsumerLinkType,
	}
	bytes, err := helper.JsonMarshal(msg)
	if err != nil {
		// TODO: 重试
		return err
	}
	_, err = r.Write([]byte{engine.RegisterMessageType})
	_, err = r.Write(bytes)
	err = r.Drain()
	if err != nil {
		// TODO: 重试
		return err
	}

	c.handler.WhenConnected()
	return nil
}

func (c *Consumer) OnClosed(_ *tcp.Remote) error {
	c.isConnected = true
	c.isRegister = false
	go c.handler.WhenClosed()
	return nil
}

func (c *Consumer) Handler(r *tcp.Remote) error {
	content := make([]byte, r.Len())
	i, err := r.Copy(content)
	if err != nil {
		return err
	}
	if i < 2 {
		return errors.New("content too short")
	}

	switch content[0] {
	case engine.RegisterMessageRespType:
		c.isRegister = true
	case engine.ProductionMessageType:
		go c.handleMessage(content[1:])
	}
	return nil
}

func (c *Consumer) handleMessage(content []byte) {
	msg := c.pool.Get().(*MessageRecord)
	msg.Reset()
	defer c.pool.Put(msg)

	err := helper.JsonUnmarshal(content, msg)
	if err != nil {
		return
	}

	if c.isRegister && python.Has[string](c.handler.Topics(), msg.Topic) {
		c.handler.Handler(msg)
	} else {
		// 出现脏数据
		return
	}
}

// IsConnected 与服务器是否连接成功
func (c *Consumer) IsConnected() bool { return c.isConnected }

// IsRegister 消费者是否注册成功
func (c *Consumer) IsRegister() bool { return c.isRegister }

// StatusOK 连接状态是否正常
func (c *Consumer) StatusOK() bool { return c.isConnected && c.isRegister }

func (c *Consumer) start() error {
	c.isRegister = false
	if c.pool == nil {
		c.pool = &sync.Pool{
			New: func() any { return &MessageRecord{} },
		}
	}

	return c.link.Connect()
}

func NewAsyncConsumer(conf Config, handler ConsumerHandler) (*Consumer, error) {
	if handler == nil || len(handler.Topics()) < 1 {
		return nil, ErrTopicEmpty
	}

	if handler.Handler == nil {
		return nil, ErrConsumerHandlerIsNil
	}

	con := &Consumer{
		link: &Link{
			conf: &Config{
				Host: conf.Host,
				Port: conf.Port,
				Ack:  engine.AllConfirm,
			},
			handler: nil,
			mu:      &sync.Mutex{},
		},
		handler: handler,
		pool: &sync.Pool{
			New: func() any { return &MessageRecord{} },
		},
	}
	con.link.handler = con // TCP 消息处理接口

	return con, con.start()
}
