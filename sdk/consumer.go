package sdk

import (
	"bytes"
	"fmt"
	"github.com/Chendemo12/fastapi-tool/helper"
	"github.com/Chendemo12/functools/python"
	"github.com/Chendemo12/functools/tcp"
	"github.com/Chendemo12/synshare-mq/src/proto"
	"sync"
	"sync/atomic"
)

type ConsumerHandler interface {
	Topics() []string
	Handler(record *proto.ConsumerMessage)
	OnConnected() // 当连接成功时,发出的信号, 此事件必须在执行完成之后才会进行后续的处理，因此需自行控制
	OnClosed()    // 当连接中断时,发出的信号, 此事件会异步执行，与重连操作（若有）可能会同步进行
}

// Consumer 消费者
type Consumer struct {
	conf        *Config         //
	link        *Link           // 底层数据连接
	handler     ConsumerHandler // 消息处理方法
	isConnected *atomic.Bool    // tcp是否连接成功
	isRegister  *atomic.Bool    // 是否注册成功
	frameBytes  []byte
	mu          *sync.Mutex
}

// HandlerFunc 获取注册的消息处理方法
func (c *Consumer) HandlerFunc() ConsumerHandler { return c.handler }

func (c *Consumer) ReRegister(r *tcp.Remote) error {
	c.isRegister.Store(false)
	_, err := r.Write(c.frameBytes)
	err = r.Drain()
	return err
}

// OnAccepted 当TCP连接成功时就发送注册消息
func (c *Consumer) OnAccepted(r *tcp.Remote) error {
	c.isConnected.Store(true)

	// 连接成功,发送注册消息
	_, err := r.Write(c.frameBytes)
	err = r.Drain()
	if err != nil {
		return err
	}

	c.handler.OnConnected()
	return nil
}

func (c *Consumer) OnClosed(_ *tcp.Remote) error {
	c.isConnected.Store(true)
	c.isRegister.Store(true)
	go c.handler.OnClosed()
	return nil
}

func (c *Consumer) Handler(r *tcp.Remote) error {
	frame := framePool.Get()
	err := frame.ParseFrom(r)
	if err != nil {
		return fmt.Errorf("tcp frame parse failed, %v", err)
	}

	switch frame.Type {
	case proto.RegisterMessageRespType:
		c.isRegister.Store(true)
		framePool.Put(frame)

	case proto.ReRegisterMessageType:
		c.isRegister.Store(false)
		return c.ReRegister(r)

	case proto.CMessageType:
		go c.handleMessage(frame)
	}
	return nil
}

// 将消息帧转换为消费者消息，中间经过了一个协议转换
func (c *Consumer) toCMessage(frame *proto.TransferFrame) ([]*proto.ConsumerMessage, error) {

	var err error
	cms := make([]*proto.ConsumerMessage, 0)
	reader := bytes.NewReader(frame.Data)

	for err == nil && reader.Len() > 0 {
		ecm := emPool.GetCM()
		err = ecm.ParseFrom(reader)
		if err == nil {
			cm := mPool.GetCM()
			cm.ParseFromCMessage(ecm)

			cms = append(cms, cm)
		}

		emPool.PutCM(ecm)
	}

	return cms, err
}

func (c *Consumer) handleMessage(frame *proto.TransferFrame) {
	defer framePool.Put(frame)

	// 转换消息格式
	cms, err := c.toCMessage(frame)
	if err != nil {
		// 后期应增加日志记录
		return
	}

	for _, cm := range cms {
		msg := cm
		go func() {
			if c.isRegister.Load() && python.Has[string](c.handler.Topics(), msg.Topic) {
				c.handler.Handler(msg)
			} else {
				// 出现脏数据
				return
			}
			defer mPool.PutCM(msg)
		}()
	}
}

// IsConnected 与服务器是否连接成功
func (c *Consumer) IsConnected() bool { return c.isConnected.Load() }

// IsRegistered 消费者是否注册成功
func (c *Consumer) IsRegistered() bool { return c.isRegister.Load() }

// StatusOK 连接状态是否正常
func (c *Consumer) StatusOK() bool { return c.isConnected.Load() && c.isRegister.Load() }

// Start 异步启动
func (c *Consumer) Start() error {
	c.isRegister.Store(false)
	c.isConnected.Store(false)

	return c.link.Connect()
}

// ==================================== methods shortcut ====================================

// JSONUnmarshal 反序列化方法
func (c *Consumer) JSONUnmarshal(data []byte, v any) error {
	return helper.JsonUnmarshal(data, v)
}

func (c *Consumer) JSONMarshal(v any) ([]byte, error) {
	return helper.JsonMarshal(v)
}

func NewConsumer(conf Config, handler ConsumerHandler) (*Consumer, error) {
	if handler == nil || len(handler.Topics()) < 1 {
		return nil, ErrTopicEmpty
	}

	if handler == nil || handler.Handler == nil {
		return nil, ErrConsumerHandlerIsNil
	}
	c := &Config{
		Host:   conf.Host,
		Port:   conf.Port,
		Ack:    conf.Ack,
		Ctx:    conf.Ctx,
		Logger: conf.Logger,
	}
	c.Clean()

	frame := framePool.Get()
	defer framePool.Put(frame)

	reporter := &proto.RegisterMessage{
		Topics: handler.Topics(),
		Ack:    proto.AllConfirm,
		Type:   proto.ConsumerLinkType,
	}

	_bytes, err := frame.BuildFrom(reporter)
	if err != nil {
		return nil, err
	}

	con := &Consumer{
		conf:    c,
		handler: handler,
		link: &Link{
			Host:    c.Host,
			Port:    c.Port,
			Kind:    proto.ConsumerLinkType,
			handler: nil,
			logger:  c.Logger,
		},
	}

	con.frameBytes = _bytes
	con.link.handler = con // TCP 消息处理接口

	return con, nil
}

func NewAsyncConsumer(conf Config, handler ConsumerHandler) (*Consumer, error) {
	con, err := NewConsumer(conf, handler)
	if err != nil {
		return nil, err
	}

	return con, con.Start()
}
