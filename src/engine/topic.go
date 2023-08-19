package engine

import (
	"encoding/binary"
	"github.com/Chendemo12/micromq/src/proto"
	"sync"
	"time"
)

var cpmp = proto.NewCPMPool()
var framePool = proto.NewFramePool()

type HistoryRecord struct {
	Topic       []byte               // 历史记录所属的topic
	MessageType proto.MessageType    // CM协议类型,以此来反序列化
	Offset      []byte               // 历史记录所属的偏移量
	Frame       *proto.TransferFrame //
	Stream      []byte               // 保留消息帧的二进制序列
	Time        int64                // 历史记录创建时间戳,而非CM被创建的事件戳
}

type Topic struct {
	Name           []byte               `json:"name"`         // 唯一标识
	HistorySize    int                  `json:"history_size"` // 生产者消息缓冲区大小
	Offset         uint64               `json:"offset"`       // 当前数据偏移量,仅用于模糊显示
	counter        *proto.Counter       // 生产者消息计数器,用于计算数据偏移量
	consumers      *sync.Map            // 全部消费者: {addr: Consumer}
	queue          chan *proto.CMessage // 等待消费者消费的数据
	historyRecords *proto.Queue         // proto.Queue[*HistoryRecord], 历史消息,由web查询展示
	mu             *sync.Mutex
	onConsumed     func(record *HistoryRecord)
}

// 计算当前消息偏移量
func (t *Topic) refreshOffset() uint64 {
	t.Offset = t.counter.ValueBeforeIncrement()
	return t.Offset
}

// 当一个消息发送给所有消费者后需要处理的事件
func (t *Topic) onMessageConsumed(record *HistoryRecord) {
	// 缓存帧序列数据
	record.Stream = record.Frame.Build()
	framePool.Put(record.Frame)
	record.Frame = nil

	// 添加到历史记录
	t.historyRecords.Append(record)

	t.onConsumed(record)
}

// 发送并等待所有消费者收到消息
func (t *Topic) sendAndWait(record *HistoryRecord) {
	wg := &sync.WaitGroup{}

	t.consumers.Range(func(key, value any) bool {
		c, ok := value.(*Consumer)
		if ok {
			wg.Add(1)
			go func() {
				defer wg.Done()

				c.mu.Lock() // 保证线程安全
				_, _ = record.Frame.WriteTo(c.Conn)
				_ = c.Conn.Drain()
				c.mu.Unlock()
			}()
		}
		return true
	})

	wg.Wait() // 所有消费者都收到了消息,触发事件
	t.onMessageConsumed(record)
}

// 向消费者发送消息帧
func (t *Topic) consume() {
	t.historyRecords = proto.NewQueue(t.HistorySize)

	for cm := range t.queue {
		frame := framePool.Get()
		// TODO: 实现多个消息压缩为帧
		//err := proto.FrameCombine[*proto.CMessage](frame, []*proto.CMessage{cm})
		err := frame.BuildFrom(cm) // TODO: 加密
		cpmp.PutCM(cm)

		if err != nil { // 消息构建失败, 增加日志记录
			continue
		}

		record := &HistoryRecord{
			Topic:       t.Name,
			Offset:      cm.Offset,
			MessageType: cm.MessageType(),
			Frame:       frame,
			Time:        time.Now().Unix(),
		}

		// cm:
		//	1. Topic.Publisher 创建, 并绑定pm
		//	2. Publisher 加入到 Topic.queue
		//	3. Topic.consume 从 Topic.queue 中消费 cm
		//	4. 此步骤构建cm二进制序列到frame上并释放CM时会同时释放PM
		go t.sendAndWait(record)
	}
}

// IterConsumer 逐个迭代现有消费者
func (t *Topic) IterConsumer(fn func(c *Consumer)) {
	t.consumers.Range(func(key, value any) bool {
		c, ok := value.(*Consumer)
		if ok {
			fn(c)
		}
		return true
	})
}

// AddConsumer 添加一个消费者
func (t *Topic) AddConsumer(con *Consumer) {
	t.consumers.Store(con.Addr, con)
}

// RemoveConsumer 移除一个消费者
func (t *Topic) RemoveConsumer(addr string) {
	t.consumers.Delete(addr)
}

// Publisher 发布消费者消息,此处会将来自生产者的消息转换成消费者消息
func (t *Topic) Publisher(pm *proto.PMessage) uint64 {
	offset := t.refreshOffset()
	cm := cpmp.GetCM() // cm.PM is nil

	binary.BigEndian.PutUint64(cm.Offset, offset)
	binary.BigEndian.PutUint64(cm.ProductTime, uint64(time.Now().Unix()))
	cm.PM = pm

	// pm:
	//	1. Engine.handlePMessage 创建
	//	2. 1调用 Engine.Publisher 传递给 Topic.Publisher
	//	3. Topic.Publisher 绑定到cm上
	//	4. CPMPool 释放CM时会同时释放PM
	//

	t.queue <- cm

	return offset
}

func NewTopic(name []byte, bufferSize, historySize int, onConsumed func(record *HistoryRecord)) *Topic {
	t := &Topic{
		Name:        name,
		HistorySize: historySize,
		Offset:      0,
		counter:     proto.NewCounter(),
		consumers:   &sync.Map{},
		queue:       make(chan *proto.CMessage, bufferSize),
		mu:          &sync.Mutex{},
		onConsumed:  onConsumed,
	}
	go t.consume()

	return t
}
