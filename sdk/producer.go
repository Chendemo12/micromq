package sdk

import (
	"github.com/Chendemo12/fastapi-tool/helper"
	"github.com/Chendemo12/functools/tcp"
	"github.com/Chendemo12/synshare-mq/src/proto"
	"sync"
)

type Producer struct {
	link        *Link // 底层数据连接
	isConnected bool
	isRegister  bool // 是否注册成功
	pool        *proto.PMessagePool
	queue       chan *proto.ProducerMessage
}

func (p *Producer) OnAccepted(r *tcp.Remote) error {
	p.isConnected = true

	msg := &proto.RegisterMessage{
		Topics: []string{},
		Ack:    proto.AllConfirm,
		Type:   proto.ProducerLinkType,
	}
	bytes, err := helper.JsonMarshal(msg)
	if err != nil {
		return err
	}
	_, err = r.Write([]byte{proto.RegisterMessageType})
	_, err = r.Write(bytes)

	return r.Drain()
}

func (p *Producer) OnClosed(r *tcp.Remote) error {
	p.isConnected = false
	return nil
}

func (p *Producer) Handler(r *tcp.Remote) error {
	if p.link.conf.Ack == proto.NoConfirm {
		return nil
	}
	// TODO: ack 未实现
	return nil
}

func (p *Producer) NewRecord() *proto.PMessage { return p.pool.Get() }

func (p *Producer) Send(msg *proto.PMessage) error {
	if msg.Topic == "" {
		return ErrTopicEmpty
	}
	p.queue <- msg
	return nil
}

func (p *Producer) Publisher(msg *proto.PMessage) error { return p.Send(msg) }

func (p *Producer) send() {
	for msg := range p.queue {
		bytes, err := helper.JsonMarshal(msg)
		p.pool.Put(msg)
		if err != nil {
			continue
		}
		_, err = p.link.client.Write([]byte{proto.PMessageType})
		_, err = p.link.client.Write(bytes)
		go func() { // 异步发送消息
			err = p.link.client.Drain()
		}()
	}
}

func (p *Producer) start() error {
	err := p.link.Connect()
	if err != nil {
		// 连接服务器失败
		return err
	}

	go p.send()

	return nil
}

func NewAsyncProducer(conf Config) (*Producer, error) {
	p := &Producer{
		queue: make(chan *proto.PMessage, 10),
		pool:  proto.NewMessagePool(),
	}
	p.link = &Link{
		kind:    proto.ProducerLinkType,
		conf:    &Config{Host: conf.Host, Port: conf.Port},
		handler: p,
		mu:      &sync.Mutex{},
	}

	return p, p.start()
}
