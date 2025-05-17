package sdk

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/Chendemo12/functools/logger"
	"github.com/Chendemo12/functools/python"
	"github.com/Chendemo12/micromq/src/proto"
	"github.com/Chendemo12/micromq/src/transfer"
)

type ConsumerHandler interface {
	ProducerHandler
	Topics() []string
	Handler(record *ConsumerMessage) // （异步执行）
}

type CHandler struct{}

func (c *CHandler) OnConnected()      {}
func (c *CHandler) OnClosed()         {}
func (c *CHandler) OnRegistered()     {}
func (c *CHandler) OnRegisterExpire() {}

func (c *CHandler) OnRegisterFailed(_ proto.MessageResponseStatus) {}

func (c *CHandler) Handler(_ *ConsumerMessage) {}

func (c *CHandler) OnNotImplementMessageType(_ *proto.TransferFrame, _ transfer.Conn) {}

// Consumer 消费者
type Consumer struct {
	broker  *Broker
	handler ConsumerHandler // 消息处理方法
	mu      *sync.Mutex
}

func (client *Consumer) handleMessage(frame *proto.TransferFrame) {
	// 将消息帧转换为消费者消息，中间经过了一个协议转换
	serverCMs := make([]*proto.CMessage, 0)
	err := proto.FrameSplit[*proto.CMessage](frame, &serverCMs, client.Crypto().Decrypt)
	if err != nil {
		// 消息提取失败
		client.Logger().Warn(frame.Text(), " decrypt or parse failed: ", err.Error())
		return
	}
	// 转换消息格式
	cms := make([]*ConsumerMessage, len(serverCMs))
	for i := 0; i < len(serverCMs); i++ {
		cms[i] = hmPool.GetCM()
		cms[i].ParseFromCMessage(serverCMs[i])
	}

	for _, msg := range cms {
		cm := msg
		go func() {
			defer hmPool.PutCM(cm)
			// 出现脏数据
			if client.broker.IsRegistered() && python.Has[string](client.handler.Topics(), cm.Topic) {
				client.handler.Handler(cm)
			} else {
				return
			}
		}()
	}
}

func (client *Consumer) distribute(frame *proto.TransferFrame, con transfer.Conn) {

	switch frame.Type() {
	case proto.CMessageType:
		client.handleMessage(frame)

	default: // 未识别的帧类型
		client.handler.OnNotImplementMessageType(frame, con)
	}
}

// ================================ interface ===============================

// IsConnected 与服务端是否连接成功
func (client *Consumer) IsConnected() bool { return client.broker.IsConnected() }

// IsRegistered 向服务端注册消费者是否成功
func (client *Consumer) IsRegistered() bool { return client.broker.IsRegistered() }

// StatusOK 连接状态是否正常,以及是否可以向服务器发送消息
func (client *Consumer) StatusOK() bool { return client.broker.StatusOK() }

// HeartbeatInterval 心跳周期
func (client *Consumer) HeartbeatInterval() time.Duration {
	return client.broker.HeartbeatInterval()
}

func (client *Consumer) Logger() logger.Iface { return client.broker.Logger() }

func (client *Consumer) Done() <-chan struct{} { return client.broker.Done() }

// Crypto 全局加密器
func (client *Consumer) Crypto() proto.Crypto { return client.broker.crypto }

// TokenCrypto Token加解密器，亦可作为全局加解密器
func (client *Consumer) TokenCrypto() *proto.TokenCrypto { return client.broker.tokenCrypto }

// SetCrypto 修改全局加解密器, 必须在 Serve 之前设置
func (client *Consumer) SetCrypto(crypto proto.Crypto) *Consumer {
	client.broker.SetCrypto(crypto)

	return client
}

// SetCryptoPlan 设置加密方案
//
//	@param	option	string		加密方案, 支持token/no (令牌加密和不加密)
//	@param	key 	[]string	其他加密参数
func (client *Consumer) SetCryptoPlan(option string, key ...string) *Consumer {
	client.broker.SetCryptoPlan(option, key...)

	return client
}

// HandlerFunc 获取注册的消息处理方法
func (client *Consumer) HandlerFunc() ConsumerHandler { return client.handler }

// Start 异步启动
func (client *Consumer) Start() error {
	client.broker.init()

	err := client.broker.link.Connect()
	if err != nil {
		// 连接服务器失败
		return err
	}

	go client.broker.HeartbeatTask()

	return nil
}

func (client *Consumer) Stop() {
	client.broker.cancel()
	_ = client.broker.link.Close()
}

// ==================================== methods shortcut ====================================

// JSONUnmarshal 反序列化方法
func (client *Consumer) JSONUnmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

func (client *Consumer) JSONMarshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

// NewConsumer 创建一个消费者，需要手动Start
func NewConsumer(conf Config, handler ConsumerHandler) (*Consumer, error) {
	if handler == nil || len(handler.Topics()) < 1 {
		return nil, ErrTopicEmpty
	}

	if handler.Handler == nil {
		return nil, ErrConsumerHandlerIsNil
	}

	c := &Config{
		Host:   conf.Host,
		Port:   conf.Port,
		Ack:    conf.Ack,
		PCtx:   conf.PCtx,
		Logger: conf.Logger,
		Token:  proto.CalcSHA(conf.Token),
	}
	c.clean()

	con := &Consumer{handler: handler, mu: &sync.Mutex{}}
	con.broker = &Broker{
		conf:           c,
		linkType:       proto.ConsumerLinkType,
		event:          handler,
		messageHandler: con.distribute,
	}

	con.broker.SetTransfer("tcp") // TODO: 目前仅支持TCP
	con.broker.SetRegisterMessage(&proto.RegisterMessage{
		Topics: handler.Topics(),
	})

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
