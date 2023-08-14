package sdk

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/Chendemo12/fastapi-tool/helper"
	"github.com/Chendemo12/fastapi-tool/logger"
	"github.com/Chendemo12/functools/python"
	"github.com/Chendemo12/functools/tcp"
	"github.com/Chendemo12/micromq/src/proto"
	"github.com/Chendemo12/micromq/src/transfer"
	"io"
	"strings"
	"sync"
	"sync/atomic"
)

type ConsumerHandler interface {
	Topics() []string
	Handler(record *proto.ConsumerMessage)
	OnConnected()      // （同步执行）当连接成功时,发出的信号, 此事件必须在执行完成之后才会进行后续的处理，因此需自行控制
	OnClosed()         // （同步执行）当连接中断时,发出的信号, 此事件必须在执行完成之后才会进行重连操作（若有）
	OnRegisterFailed() // （同步执行）当注册失败触发的事件
	// OnNotImplementMessageType 当收到一个未实现的消息帧时触发的事件
	OnNotImplementMessageType(frame *proto.TransferFrame, c transfer.Conn)
}

type CHandler struct{}

func (c *CHandler) OnConnected()      {}
func (c *CHandler) OnClosed()         {}
func (c *CHandler) OnRegisterFailed() {}

func (c *CHandler) OnNotImplementMessageType(frame *proto.TransferFrame, con transfer.Conn) {}

// ----------------------------------------------------------------------------

// Consumer 消费者
type Consumer struct {
	conf        *Config         //
	link        Link            // 底层数据连接
	handler     ConsumerHandler // 消息处理方法
	isConnected *atomic.Bool    // tcp是否连接成功
	isRegister  *atomic.Bool    // 是否注册成功
	frameBytes  []byte
	mu          *sync.Mutex
}

// 将消息帧转换为消费者消息，中间经过了一个协议转换
func (c *Consumer) toCMessage(frame *proto.TransferFrame) ([]*proto.ConsumerMessage, error) {

	var err error
	cms := make([]*proto.ConsumerMessage, 0)
	reader := bytes.NewReader(frame.Data)

	for err == nil && reader.Len() > 0 {
		serverCM := emPool.GetCM()
		serverCM.PM = emPool.GetPM()
		err = serverCM.ParseFrom(reader)

		if err == nil {
			cm := hmPool.GetCM()
			cm.ParseFromCMessage(serverCM)
			cms = append(cms, cm)
		}

		emPool.PutCM(serverCM)
	}

	return cms, err
}

func (c *Consumer) handleMessage(frame *proto.TransferFrame) {
	// 转换消息格式
	cms, err := c.toCMessage(frame)
	if err != nil {
		// 记录日志
		c.Logger().Warn(fmt.Sprintf("%s parse failed: %s", frame, err))
		return
	}

	for _, msg := range cms {
		cm := msg
		go func() {
			defer hmPool.PutCM(cm)
			// 出现脏数据
			if c.isRegister.Load() && python.Has[string](c.handler.Topics(), cm.Topic) {
				c.handler.Handler(cm)
			} else {
				return
			}
		}()
	}
}

func (c *Consumer) handleRegisterMessage(frame *proto.TransferFrame, con transfer.Conn) {
	form := &proto.MessageResponse{}
	err := frame.Unmarshal(form)
	if err != nil {
		c.Logger().Warn("register message response unmarshal failed: ", err.Error())
		_ = c.ReRegister(con) // retry
		return
	}

	// 处理注册响应, 目前由服务器保证重新注册等流程
	if form.Status == proto.AcceptedStatus {
		c.Logger().Info("consumer register successfully")
	} else {
		c.Logger().Warn("consumer register failed: ", proto.GetMessageResponseStatusText(form.Status))
	}

	switch form.Status {
	case proto.AcceptedStatus:
		c.isRegister.Store(true)
	case proto.RefusedStatus:
		c.isRegister.Store(false)
	case proto.TokenIncorrectStatus:
		c.isRegister.Store(false)
	}
}

func (c *Consumer) distribute(frame *proto.TransferFrame, con transfer.Conn) {
	switch frame.Type {

	case proto.RegisterMessageRespType:
		c.handleRegisterMessage(frame, con)

	case proto.ReRegisterMessageType:
		c.isRegister.Store(false)
		c.Logger().Debug("sever let re-register")
		_ = c.ReRegister(con)

	case proto.CMessageType:
		c.handleMessage(frame)

	default: // 未识别的帧类型
		c.handler.OnNotImplementMessageType(frame, con)
	}
}

func (c *Consumer) Logger() logger.Iface { return c.conf.Logger }

// HandlerFunc 获取注册的消息处理方法
func (c *Consumer) HandlerFunc() ConsumerHandler { return c.handler }

func (c *Consumer) ReRegister(r transfer.Conn) error {
	c.isRegister.Store(false)
	_, err := r.Write(c.frameBytes)
	err = r.Drain()

	return err
}

// ================================ TCP handler ===============================

// OnAccepted 当TCP连接成功时会自行发送注册消息
func (c *Consumer) OnAccepted(r *tcp.Remote) error {
	c.isConnected.Store(true)

	c.handler.OnConnected()

	// 连接成功,发送注册消息
	return c.ReRegister(r)
}

func (c *Consumer) OnClosed(r *tcp.Remote) error {
	c.Logger().Warn("consumer connection lost with: ", r.Addr())
	c.isConnected.Store(false)
	c.isRegister.Store(false)
	c.handler.OnClosed()

	return nil
}

func (c *Consumer) Handler(r *tcp.Remote) error {
	if r.Len() < proto.FrameMinLength {
		return proto.ErrMessageNotFull
	}

	frame := framePool.Get()
	err := frame.ParseFrom(r) // 此操作不应并发读取，避免消息2覆盖消息1的缓冲区

	// 异步执行，立刻读取下一条消息
	go func(f *proto.TransferFrame, con transfer.Conn, err error) { // 处理消息帧
		defer framePool.Put(f)

		if err != nil {
			if !errors.Is(err, io.EOF) {
				c.Logger().Warn(fmt.Errorf("consumer parse frame failed: %v", err))
			}
		} else {
			c.distribute(f, con)
		}
	}(frame, r, err)

	return nil
}

// ================================ UDP handler ===============================
//

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

func (c *Consumer) Stop() {
	c.isRegister.Store(false)
	c.isConnected.Store(false)
	_ = c.link.Close()
}

// ==================================== methods shortcut ====================================

// JSONUnmarshal 反序列化方法
func (c *Consumer) JSONUnmarshal(data []byte, v any) error {
	return helper.JsonUnmarshal(data, v)
}

func (c *Consumer) JSONMarshal(v any) ([]byte, error) {
	return helper.JsonMarshal(v)
}

// NewConsumer 创建一个消费者，需要手动Start
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
		Token:  proto.CalcSHA(conf.Token),
	}
	c.Clean()

	con := &Consumer{
		conf:        c,
		handler:     handler,
		isConnected: &atomic.Bool{},
		isRegister:  &atomic.Bool{},
		mu:          &sync.Mutex{},
	}

	switch strings.ToUpper(c.Link) {
	case "TCP", "UDP": // TODO: 目前仅支持TCP
		con.link = &TCPLink{
			Host:     c.Host,
			Port:     c.Port,
			LinkType: proto.ConsumerLinkType,
			handler:  nil,
			logger:   c.Logger,
		}
		con.link.SetTCPHandler(con) // TCP 消息处理接口
	default:
		return nil, errors.New("unsupported link: " + c.Link)
	}

	frame := framePool.Get()
	reporter := &proto.RegisterMessage{
		Topics: handler.Topics(),
		Ack:    proto.AllConfirm,
		Type:   proto.ConsumerLinkType,
		Token:  c.Token,
	}

	_bytes, err := frame.BuildFrom(reporter)
	framePool.Put(frame)

	if err != nil {
		return nil, err
	}

	con.frameBytes = _bytes
	return con, nil
}

// NewAsyncConsumer 创建异步消费者
func NewAsyncConsumer(conf Config, handler ConsumerHandler) (*Consumer, error) {
	con, err := NewConsumer(conf, handler)
	if err != nil {
		return nil, err
	}

	return con, con.Start()
}
