package sdk

import (
	"github.com/Chendemo12/fastapi-tool/helper"
	"github.com/Chendemo12/fastapi-tool/logger"
	"github.com/Chendemo12/micromq/src/proto"
	"github.com/Chendemo12/micromq/src/transfer"
	"time"
)

type ProducerHandler interface {
	OnConnected()                                        // （同步执行）当连接成功时触发的事件, 此事件必须在执行完成之后才会进行后续的处理，因此需自行控制
	OnClosed()                                           // （同步执行）当连接中断时触发的事件, 此事件必须在执行完成之后才会进行重连操作（若有）
	OnRegistered()                                       // （同步执行）当注册成功触发的事件
	OnRegisterFailed(status proto.MessageResponseStatus) // （同步执行）当注册失败触发的事件
	OnRegisterExpire()                                   // （同步执行）当连接中断时触发的事件, 此事件必须在执行完成之后才会进行重连操作（若有）                                  // 阻塞调用
	// OnNotImplementMessageType 当收到一个未实现的消息帧时触发的事件
	OnNotImplementMessageType(frame *proto.TransferFrame, con transfer.Conn)
}

// PHandler 默认实现
type PHandler struct{}

func (h PHandler) OnConnected()      {}
func (h PHandler) OnClosed()         {}
func (h PHandler) OnRegistered()     {}
func (h PHandler) OnRegisterExpire() {}

func (h PHandler) OnRegisterFailed(status proto.MessageResponseStatus) {}

func (h PHandler) OnNotImplementMessageType(frame *proto.TransferFrame, con transfer.Conn) {}

// Producer 生产者, 通过 Send 发送的消息并非会立即投递给服务端
// 而是会按照服务器下发的配置定时批量发送消息,通常为500ms
type Producer struct {
	broker   *Broker
	queue    chan *proto.ProducerMessage
	handler  ProducerHandler
	dingDong chan struct{}
	ackTime  time.Time
}

// 收到来自服务端的消息发送成功确认消息
func (client *Producer) receiveFin() {
	client.ackTime = time.Now()
	if client.broker.conf.Ack == proto.NoConfirm {
		// TODO: ack 未实现
	}
}

// 每滴答一次，就产生一个数据发送信号
func (client *Producer) tick() {
	for {
		select {
		case <-client.Done():
			return
		default:
			// 此操作以支持实时修改发送周期
			time.Sleep(client.broker.TickerInterval())
			client.dingDong <- struct{}{} // 发送信号
		}
	}
}

func (client *Producer) sendToServer() {
	var rate byte = 2
	for {
		if !client.StatusOK() { // 客户端未连接或注册失败
			if rate > 10 {
				rate = 2
			}
			time.Sleep(client.broker.TickerInterval() * time.Duration(rate))
			rate++ // 等待时间逐渐延长
			continue
		}

		rate = 2 // 重置等待时间
		select {
		case <-client.Done():
			client.Stop()
			return

		case <-client.dingDong:

		// TODO: 定时批量发送消息
		case pm := <-client.queue:
			serverPM := &proto.PMessage{
				Topic: helper.S2B(pm.Topic),
				Key:   helper.S2B(pm.Key),
				Value: pm.Value,
			}

			frame := framePool.Get()
			_bytes, err := frame.BuildFrom(serverPM)

			// release
			framePool.Put(frame)
			hmPool.PutPM(pm)

			if err != nil { // 可能性很小
				continue
			}

			go func() { // 异步发送消息
				_, err2 := client.broker.link.Write(_bytes)
				err2 = client.broker.link.Drain()
				if err2 != nil {
					client.Logger().Warn("send message to server failed: ", err2)
				}
			}()
		}
	}
}

func (client *Producer) distribute(frame *proto.TransferFrame, r transfer.Conn) {
	switch frame.Type {

	case proto.MessageRespType:
		client.receiveFin()

	default: // 未识别的帧类型
		client.handler.OnNotImplementMessageType(frame, r)
	}
}

// ================================ interface ===============================

// IsConnected 与服务端是否连接成功
func (client *Producer) IsConnected() bool { return client.broker.IsConnected() }

// IsRegistered 向服务端注册消费者是否成功
func (client *Producer) IsRegistered() bool { return client.broker.IsRegistered() }

// StatusOK 连接状态是否正常,以及是否可以向服务器发送消息
func (client *Producer) StatusOK() bool { return client.broker.StatusOK() }

// HeartbeatInterval 心跳周期
func (client *Producer) HeartbeatInterval() time.Duration {
	return client.broker.HeartbeatInterval()
}

func (client *Producer) Logger() logger.Iface { return client.broker.Logger() }

func (client *Producer) Done() <-chan struct{} { return client.broker.Done() }

// NewRecord 从池中初始化一个新的消息记录
func (client *Producer) NewRecord() *proto.ProducerMessage {
	return hmPool.GetPM()
}

// PutRecord 主动归还消息记录到池，仅在主动调用 NewRecord 却没发送数据时使用
func (client *Producer) PutRecord(msg *proto.ProducerMessage) {
	hmPool.PutPM(msg)
}

// Publisher 发送消息
func (client *Producer) Publisher(msg *proto.ProducerMessage) error {
	if msg.Topic == "" {
		client.PutRecord(msg)
		return ErrTopicEmpty
	}

	if !client.broker.IsConnected() {
		client.PutRecord(msg)
		return ErrProducerUnconnected
	}
	// 未注册成功，禁止发送消息
	if !client.broker.IsRegistered() {
		client.PutRecord(msg)
		return ErrProducerUnregistered
	}

	client.queue <- msg
	return nil
}

// Send 发送一条消息
func (client *Producer) Send(fn func(record *proto.ProducerMessage) error) error {
	msg := client.NewRecord()
	err := fn(msg)
	if err != nil {
		client.PutRecord(msg)
		return err
	}
	return client.Publisher(msg)
}

func (client *Producer) Start() error {
	err := client.broker.link.Connect()
	if err != nil {
		// 连接服务器失败
		return err
	}

	go client.tick()
	go client.sendToServer()
	go client.broker.HeartbeatTask()

	return nil
}

func (client *Producer) Stop() {
	client.broker.cancel()
	_ = client.broker.link.Close()
}

// ==================================== methods shortcut ====================================

// JSONMarshal 序列化方法
func (client *Producer) JSONMarshal(v any) ([]byte, error) {
	return helper.JsonMarshal(v)
}

func (client *Producer) JSONUnmarshal(data []byte, v any) error {
	return helper.JsonUnmarshal(data, v)
}

// Beautify 格式化显示字节流
func (client *Producer) Beautify(data []byte) string {
	return helper.HexBeautify(data)
}

// NewProducer 创建异步生产者,需手动启动
func NewProducer(conf Config, handlers ...ProducerHandler) *Producer {
	c := &Config{
		Host:   conf.Host,
		Port:   conf.Port,
		Ack:    conf.Ack,
		PCtx:   conf.PCtx,
		Logger: conf.Logger,
		Token:  proto.CalcSHA(conf.Token),
	}
	con := &Producer{
		broker:   nil,
		queue:    make(chan *proto.ProducerMessage, 10),
		handler:  nil,
		dingDong: make(chan struct{}, 1),
		ackTime:  time.Now(),
	}
	if len(handlers) > 0 && handlers[0] != nil {
		con.handler = handlers[0]
	} else {
		con.handler = &PHandler{}
	}

	con.broker = &Broker{
		conf:           c,
		linkType:       proto.ProducerLinkType,
		event:          con.handler,
		messageHandler: con.distribute,
	}

	con.broker.init()
	con.broker.SetTransfer("tcp") // TODO: 目前仅支持TCP
	_ = con.broker.SetRegisterMessage(&proto.RegisterMessage{})

	return con
}

// NewAsyncProducer 创建异步生产者,无需再手动启动
func NewAsyncProducer(conf Config, handlers ...ProducerHandler) (*Producer, error) {
	p := NewProducer(conf, handlers...)

	return p, p.Start()
}
