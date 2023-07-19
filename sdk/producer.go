package sdk

import (
	"github.com/Chendemo12/fastapi-tool/helper"
	"github.com/Chendemo12/functools/tcp"
	"github.com/Chendemo12/synshare-mq/src/proto"
	"sync"
	"time"
)

type Producer struct {
	link          *Link // 底层数据连接
	isConnected   bool
	isRegister    bool   // 是否注册成功
	regFrameBytes []byte // 注册消息帧
	queue         chan *proto.ProducerMessage
	timer         *time.Timer
}

func (p *Producer) OnAccepted(r *tcp.Remote) error {
	p.isConnected = true
	p.isRegister = false
	_, _ = r.Write(p.regFrameBytes)

	return r.Drain()
}

func (p *Producer) OnClosed(r *tcp.Remote) error {
	p.isConnected = false
	p.isRegister = false
	return nil
}

func (p *Producer) Handler(r *tcp.Remote) error {
	if p.link.conf.Ack == proto.NoConfirm {
		return nil
	}
	// TODO: ack 未实现
	return nil
}

func (p *Producer) NewRecord() *proto.ProducerMessage {
	return mPool.GetPM()
}

func (p *Producer) Send(msg *proto.ProducerMessage) error {
	if msg.Topic == "" {
		return ErrTopicEmpty
	}
	p.queue <- msg
	return nil
}

func (p *Producer) Publisher(msg *proto.ProducerMessage) error { return p.Send(msg) }

func (p *Producer) send() {
	for {
		select {
		case <-p.timer.C:
		// TODO: 定时批量发送消息
		case msg := <-p.queue:
			serverPM := &proto.PMessage{
				Topic: helper.S2B(msg.Topic),
				Key:   helper.S2B(msg.Key),
				Value: msg.Value,
			}

			frame := framePool.Get()
			_bytes, err := frame.BuildFrom(serverPM)

			// release
			mPool.PutPM(msg)
			framePool.Put(frame)

			if err != nil {
				continue
			}

			_, err = p.link.client.Write(_bytes)
			framePool.Put(frame)

			go func() { // 异步发送消息
				err = p.link.client.Drain()
			}()
		}
	}
}

func (p *Producer) start() error {
	p.timer = time.NewTimer(time.Millisecond * 500) // 500 ms

	err := p.link.Connect()
	if err != nil {
		// 连接服务器失败
		return err
	}

	go p.send()

	return nil
}

// ==================================== methods shortcut ====================================

// JSONMarshal 序列化方法
func (p *Producer) JSONMarshal(v any) ([]byte, error) {
	return helper.JsonMarshal(v)
}

func (p *Producer) JSONUnmarshal(data []byte, v any) error {
	return helper.JsonUnmarshal(data, v)
}

// NewAsyncProducer 创建异步生产者,无需再手动启动
func NewAsyncProducer(conf Config) (*Producer, error) {
	p := &Producer{
		isConnected: false,
		isRegister:  false,
		queue:       make(chan *proto.ProducerMessage, 10),
	}
	p.link = &Link{
		kind:    proto.ProducerLinkType,
		conf:    &Config{Host: conf.Host, Port: conf.Port},
		handler: p,
		mu:      &sync.Mutex{},
	}

	frame := framePool.Get()
	_bytes, err := frame.BuildFrom(proto.NewPRegisterMessage())
	if err != nil {
		return nil, err
	}

	p.regFrameBytes = _bytes
	framePool.Put(frame)

	return p, p.start()
}
