package engine

import (
	"errors"
	"github.com/Chendemo12/fastapi-tool/helper"
	"github.com/Chendemo12/fastapi-tool/logger"
	"github.com/Chendemo12/functools/tcp"
	"github.com/Chendemo12/synshare-mq/src/proto"
)

var ErrMessageNotFull = errors.New("message is not full")

type Transfer struct {
	logger logger.Iface
	mq     *Engine
	tcps   *tcp.Server
}

func (t *Transfer) init() {
	ts := tcp.NewTcpServer(
		&tcp.TcpsConfig{
			Host:           t.mq.conf.Host,
			Port:           t.mq.conf.Port,
			MessageHandler: t,
			ByteOrder:      "big",
			Logger:         t.logger,
			MaxOpenConn:    t.mq.conf.MaxOpenConn,
		},
	)
	t.tcps = ts
}

func (t *Transfer) SetEngine(en *Engine) *Transfer {
	t.mq = en
	return t
}

func (t *Transfer) OnAccepted(r *tcp.Remote) error {
	t.logger.Info(r.Addr(), "connected.")
	// 连接成功时不关联数据, 仅在注册成功时,关联到 Engine 中
	return nil
}

func (t *Transfer) OnClosed(r *tcp.Remote) error {
	for _, cons := range t.mq.consumers {
		if cons.Addr == r.Addr() {
			t.logger.Info(r.Addr(), "consumer close connection.")
			t.mq.RemoveConsumer(r.Addr())
			t.mq.consumers[r.Index()] = nil
			return nil
		}
	}

	for _, prod := range t.mq.producers {
		if prod.Addr == r.Addr() {
			t.logger.Info(r.Addr(), "producer close connection.")
			t.mq.producers[r.Index()] = nil
		}
	}

	return nil
}

// Distribute 读取第一个字节,判断并分发消息
func (t *Transfer) Distribute(content []byte, r *tcp.Remote) {
	var resp []byte
	var err error
	respType := make([]byte, 1)

	switch content[0] {
	case proto.RegisterMessageType: // 注册消费者
		resp, err = t.HandleRegisterMessage(content[1:], r)
		respType[0] = proto.RegisterMessageRespType
	case proto.ProductionMessageType: // 生产消息
		resp, err = t.HandleProductionMessage(content[1:], r)
		respType[0] = proto.ProductionMessageRespType
	}

	// 回写返回值
	if err != nil || len(resp) == 0 {
		return
	}
	// 写入消息类别
	_, err = r.Write(respType)
	_, err = r.Write(resp)
	if err != nil {
		return
	}
	err = r.Drain()
	if err != nil {
		return
	}
}

func (t *Transfer) Handler(r *tcp.Remote) error {
	if r.Len() < 2 {
		return ErrMessageNotFull
	}

	// 读取并拷贝字节流
	content := make([]byte, r.Len())
	_, err := r.Read(content)
	if err != nil {
		return err
	}

	go t.Distribute(content, r)

	return nil
}

func (t *Transfer) HandleRegisterMessage(content []byte, r *tcp.Remote) ([]byte, error) {
	msg := &proto.RegisterMessage{}
	err := helper.JsonUnmarshal(content, msg)
	if err != nil {
		return nil, err
	}

	switch msg.Type {

	case proto.ProducerLinkType: // 注册生产者
		prod := &Producer{
			Conf: &ProducerConfig{
				Ack: msg.Ack,
			},
			Addr: r.Addr(),
			Conn: r,
		}
		t.mq.producers[r.Index()] = prod

	case proto.ConsumerLinkType: // 注册消费者
		cons := &Consumer{
			Conf: &ConsumerConfig{
				Topics: msg.Topics,
				Ack:    msg.Ack,
			},
			Addr: r.Addr(),
			Conn: r,
		}

		t.mq.consumers[r.Index()] = cons

		for _, name := range msg.Topics {
			t.mq.GetTopic(name).AddConsumer(cons)
		}
	}

	return nil, nil
}

func (t *Transfer) HandleProductionMessage(content []byte, r *tcp.Remote) ([]byte, error) {
	msg := mPool.Get()

	err := helper.JsonUnmarshal(content, msg)
	if err != nil {
		mPool.Put(msg)
		return nil, err
	}

	offset := t.mq.GetTopic(msg.Topic).Publisher(msg)
	consumer := t.mq.consumers[r.Index()]

	if consumer != nil && consumer.NeedConfirm() {
		// 需要返回确认消息给客户端
		resp := mrPool.Get()
		defer mrPool.Put(resp)

		resp.ReceiveTime = msg.ProductTime
		resp.Offset = offset
		resp.Result = true

		return helper.JsonMarshal(resp)
	}

	return nil, nil
}

func (t *Transfer) Start() error { return t.tcps.Serve() }

func (t *Transfer) Stop() {
	t.tcps.Stop()
	t.logger.Info("server stopped!")
}
