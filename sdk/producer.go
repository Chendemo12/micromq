package sdk

import (
	"github.com/Chendemo12/fastapi-tool/helper"
	"github.com/Chendemo12/functools/tcp"
	"github.com/Chendemo12/synshare-mq/src/engine"
	"sync"
)

type ProductionMessage = engine.Message
type ProductionMessageResp = engine.MessageResponse

type Producer struct {
	link        *Link // 底层数据连接
	isConnected bool
	isRegister  bool // 是否注册成功
	pool        *sync.Pool
	queue       chan *ProductionMessage
}

func (p *Producer) OnAccepted(r *tcp.Remote) error {
	p.isConnected = true

	msg := engine.RegisterMessage{
		Topics: []string{},
		Ack:    engine.AllConfirm,
		Type:   engine.ProducerLinkType,
	}
	bytes, err := helper.JsonMarshal(msg)
	if err != nil {
		return err
	}
	_, err = r.Write([]byte{engine.RegisterMessageType})
	_, err = r.Write(bytes)

	return r.Drain()
}

func (p *Producer) OnClosed(r *tcp.Remote) error {
	p.isConnected = false
	return nil
}

func (p *Producer) Handler(r *tcp.Remote) error {
	if p.link.conf.Ack == engine.NoConfirm {
		return nil
	}
	// TODO: ack 未实现
	return nil
}

func (p *Producer) NewRecord() *ProductionMessage {
	return p.pool.Get().(*ProductionMessage)
}

func (p *Producer) Send(msg *ProductionMessage) error {
	if msg.Topic == "" {
		return ErrTopicEmpty
	}
	p.queue <- msg
	return nil
}

func (p *Producer) Publisher(msg *ProductionMessage) error { return p.Send(msg) }

func (p *Producer) send() {
	for msg := range p.queue {
		bytes, err := helper.JsonMarshal(msg)
		p.pool.Put(msg)
		if err != nil {
			continue
		}
		// TODO: client 未实现发数据
		_, err = p.link.client.Write([]byte{engine.ProductionMessageType})
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
		queue: make(chan *ProductionMessage, 10),
		pool: &sync.Pool{
			New: func() any { return &ProductionMessage{} },
		},
	}
	p.link = &Link{
		kind:    engine.ProducerLinkType,
		conf:    &Config{Host: conf.Host, Port: conf.Port},
		handler: p,
		mu:      &sync.Mutex{},
	}

	return p, p.start()
}
