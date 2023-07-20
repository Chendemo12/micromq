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
	ticker        *time.Ticker
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

// NewRecord 从池中初始化一个新的消息记录
func (p *Producer) NewRecord() *proto.ProducerMessage {
	return mPool.GetPM()
}

// PutRecord 主动归还消息记录到池，仅在主动调用 NewRecord 却没发送数据时使用
func (p *Producer) PutRecord(msg *proto.ProducerMessage) {
	mPool.PutPM(msg)
}

func (p *Producer) Publisher(msg *proto.ProducerMessage) error {
	if msg.Topic == "" {
		mPool.PutPM(msg)
		return ErrTopicEmpty
	}
	p.queue <- msg
	return nil
}

// Send 发送一条消息
func (p *Producer) Send(fn func(record *proto.ProducerMessage) error) error {
	msg := p.NewRecord()
	err := fn(msg)
	if err != nil {
		mPool.PutPM(msg)
		return err
	}
	return p.Publisher(msg)
}

func (p *Producer) send() {
	for {
		select {
		case <-p.ticker.C:
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
	p.ticker = time.NewTicker(time.Millisecond * 500) // 500 ms

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

// Beautify 格式化显示字节流
func (p *Producer) Beautify(data []byte) string {
	return helper.HexBeautify(data)
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
