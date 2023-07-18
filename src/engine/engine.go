package engine

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/Chendemo12/fastapi-tool/helper"
	"github.com/Chendemo12/fastapi-tool/logger"
	"github.com/Chendemo12/functools/tcp"
	"github.com/Chendemo12/synshare-mq/src/proto"
	"os"
	"os/signal"
	"sync"
	"time"
)

type Config struct {
	Host        string       `json:"host"`
	Port        string       `json:"port"`
	MaxOpenConn int          `json:"max_open_conn"`
	BufferSize  int          `json:"buffer_size"`
	Logger      logger.Iface `json:"-"`
}

type Engine struct {
	conf      *Config
	producers []*Producer // 生产者
	consumers []*Consumer // 消费者
	topics    *sync.Map
	transfer  *Transfer
}

// RangeTopic if false returned, for-loop will stop
func (e *Engine) RangeTopic(fn func(topic *Topic) bool) {
	e.topics.Range(func(key, value any) bool {
		return fn(value.(*Topic))
	})
}

func (e *Engine) AddTopic(name []byte) *Topic {
	topic := NewTopic(name, e.conf.BufferSize)
	e.topics.Store(name, topic)
	return topic
}

// GetTopic 获取topic,并在不存在时自动新建一个topic
func (e *Engine) GetTopic(name []byte) *Topic {
	var topic *Topic

	v, ok := e.topics.Load(name)
	if !ok {
		topic = e.AddTopic(name)
	} else {
		topic = v.(*Topic)
	}

	return topic
}

// GetTopicOffset 查询指定topic当前的消息偏移量
func (e *Engine) GetTopicOffset(name []byte) uint64 {
	var offset uint64

	e.RangeTopic(func(topic *Topic) bool {
		if bytes.Compare(topic.Name, name) == 0 {
			offset = topic.offset
			return false
		}
		return true
	})

	return offset
}

// RemoveConsumer 删除一个消费者
func (e *Engine) RemoveConsumer(addr string) {
	for _, consumer := range e.consumers {
		if consumer == nil || consumer.Conn == nil {
			continue
		}
		if consumer.Addr == addr {
			for _, name := range consumer.Conf.Topics {
				e.GetTopic([]byte(name)).RemoveConsumer(addr)
			}
		}
	}
}

// Publisher 发布消息,并返回此消息在当前topic中的偏移量
func (e *Engine) Publisher(msg *proto.PMessage) uint64 {
	return e.GetTopic(msg.Topic).Publisher(msg)
}

// Distribute 分发消息
func (e *Engine) Distribute(frame *proto.TransferFrame, r *tcp.Remote) {
	defer framePool.Put(frame)

	var resp []byte
	var err error
	respType := make([]byte, 1)

	switch frame.Type {
	case proto.RegisterMessageType: // 注册消费者
		resp, err = e.HandleRegisterMessage(frame, r)
		respType[0] = byte(proto.RegisterMessageRespType)
	case proto.PMessageType: // 生产消息
		resp, err = e.HandleProductionMessage(frame, r)
		respType[0] = byte(proto.MessageRespType)
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

// HandleRegisterMessage 处理注册消息
func (e *Engine) HandleRegisterMessage(frame *proto.TransferFrame, r *tcp.Remote) ([]byte, error) {
	msg, err := frame.ParseTo()
	rgm, ok := msg.(*proto.RegisterMessage)
	if err != nil || !ok {
		return nil, fmt.Errorf("register message parse failed, %v", err)
	}

	switch rgm.Type {

	case proto.ProducerLinkType: // 注册生产者
		prod := &Producer{
			Conf: &ProducerConfig{Ack: rgm.Ack},
			Addr: r.Addr(),
			Conn: r,
		}
		e.producers[r.Index()] = prod // 记录生产者, 用于判断其后是否要返回消息投递后的确认消息

	case proto.ConsumerLinkType: // 注册消费者
		cons := &Consumer{
			Conf: &ConsumerConfig{Topics: rgm.Topics, Ack: rgm.Ack},
			Addr: r.Addr(),
			Conn: r,
		}

		e.consumers[r.Index()] = cons

		for _, name := range rgm.Topics {
			e.GetTopic([]byte(name)).AddConsumer(cons)
		}
	}

	return nil, nil
}

func (e *Engine) HandleProductionMessage(frame *proto.TransferFrame, r *tcp.Remote) ([]byte, error) {
	pms := make([]*proto.PMessage, 0)
	// 解析消息帧
	proto.ParsePMFrame(&pms, frame.Data)
	if len(pms) < 1 {
		return nil, errors.New("message not found in frame")
	}

	// 若是批量发送数据,则取最后一条消息的偏移量
	var offset uint64 = 0
	for _, pm := range pms {
		offset = e.GetTopic(pm.Topic).Publisher(pm)
	}

	consumer := e.consumers[r.Index()]
	if consumer != nil && consumer.NeedConfirm() {
		// 需要返回确认消息给客户端
		resp := &proto.MessageResponse{}
		resp.ReceiveTime = time.Now()
		resp.Offset = offset
		resp.Result = true

		return helper.JsonMarshal(resp)
	}

	return nil, nil
}

func (e *Engine) Run() {
	go func() {
		err := e.transfer.Start()
		if err != nil {
			e.conf.Logger.Error("server starts failed, ", err)
			os.Exit(1)
		}
	}()

	// 关闭开关, buffered
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit // 阻塞进程，直到接收到停止信号,准备关闭程序

	e.transfer.Stop()
}

func New(c ...Config) *Engine {
	var d Config

	if len(c) > 0 {
		d = c[0]
	} else {
		d = Config{
			Host:        "127.0.0.1",
			Port:        "9999",
			MaxOpenConn: 50,
			BufferSize:  200,
			Logger:      logger.NewDefaultLogger(),
		}
	}

	if d.BufferSize == 0 {
		d.BufferSize = 100
	}
	if d.Logger == nil {
		d.Logger = logger.NewDefaultLogger()
	}

	if !(d.MaxOpenConn > 0 && d.MaxOpenConn <= 100) {
		d.MaxOpenConn = 50
	}

	eng := &Engine{
		conf: &Config{
			Host:        d.Host,
			Port:        d.Port,
			MaxOpenConn: d.MaxOpenConn,
			BufferSize:  d.BufferSize,
			Logger:      d.Logger,
		},
		producers: make([]*Producer, d.MaxOpenConn),
		consumers: make([]*Consumer, d.MaxOpenConn),
		topics:    &sync.Map{},
	}

	eng.transfer = &Transfer{
		logger: d.Logger,
		mq:     eng,
	}
	eng.transfer.SetEngine(eng).init()

	return eng
}
