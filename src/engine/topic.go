package engine

import (
	"bytes"
	"encoding/binary"
	"github.com/Chendemo12/micromq/src/proto"
	"sync"
	"time"
)

var cpmp = proto.NewCPMPool()
var framePool = proto.NewFramePool()

type HistoryRecord struct {
	frame       *proto.TransferFrame //
	Topic       []byte               // 历史记录所属的topic
	Key         []byte               //
	Value       []byte               //
	Offset      uint64               // 历史记录所属的偏移量
	MessageType proto.MessageType    // CM协议类型,以此来反序列化
	Time        int64                // 历史记录创建时间戳,而非CM被创建的事件戳
	Error       string               //
}

type Topic struct {
	Name               []byte                        `json:"name"`         // 唯一标识
	ForwardingAddrs    [ForwardingAddrsMaxNum]*Topic `json:"-"`            // 数据转发目标, nil 则不存在
	ForwardingAddrsEnd int                           `json:"-"`            // ForwardingAddrs 的结束位置 < ForwardingAddrsMaxNum
	HistorySize        int                           `json:"history_size"` // 生产者消息缓冲区大小
	Offset             uint64                        `json:"offset"`       // 当前数据偏移量,仅用于模糊显示
	counter            *proto.Counter                // 生产者消息计数器,用于计算数据偏移量
	consumers          *sync.Map                     // 全部消费者: {addr: Consumer}
	queue              chan *proto.CMessage          // 等待消费者消费的数据
	historyRecords     *proto.Queue                  // proto.Queue[*HistoryRecord], 历史消息,由web查询展示
	crypto             proto.Crypto                  // 加解密器
	forwardLock        *sync.Mutex                   // ForwardingAddrs 锁
	onConsumed         func(record *HistoryRecord)
}

// 计算当前消息偏移量
func (t *Topic) refreshOffset() uint64 {
	t.Offset = t.counter.ValueBeforeIncrement()
	return t.Offset
}

// 当一个消息发送给所有消费者后需要处理的事件
func (t *Topic) onMessageConsumed(record *HistoryRecord) {
	// 缓存帧序列数据
	framePool.Put(record.frame)
	record.frame = nil

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
				_, _ = record.frame.WriteTo(c.Conn)
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
		record := &HistoryRecord{
			Topic:       t.Name,
			Offset:      binary.BigEndian.Uint64(cm.Offset),
			MessageType: cm.MessageType(),
			Time:        time.Now().Unix(),
			frame:       nil,
		}
		record.Key = make([]byte, len(cm.PM.Key))
		record.Value = make([]byte, len(cm.PM.Value))
		copy(record.Key, cm.PM.Key)
		copy(record.Value, cm.PM.Value)

		frame := framePool.Get()
		// TODO: 实现多个消息压缩为帧
		//err := proto.FrameCombine[*proto.CMessage](frame, []*proto.CMessage{cm})
		err := frame.BuildFrom(cm, t.crypto.Encrypt)
		cpmp.PutCM(cm)

		if err != nil { // 消息构建失败, 增加日志记录
			record.Error = err.Error()
			continue
		}

		record.frame = frame

		// cm:
		//	1. Topic.Publisher 创建, 并绑定pm
		//	2. Publisher 加入到 Topic.queue
		//	3. Topic.consume 从 Topic.queue 中消费 cm
		//	4. 此步骤构建cm二进制序列到frame上并释放CM时会同时释放PM
		go t.sendAndWait(record)
	}
}

// RangeConsumer 逐个迭代内部消费者
func (t *Topic) RangeConsumer(fn func(c *Consumer)) {
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

func (t *Topic) SetOnConsumed(onConsumed func(record *HistoryRecord)) *Topic {
	t.onConsumed = onConsumed

	return t
}

func (t *Topic) SetCrypto(crypto proto.Crypto) *Topic {
	t.crypto = crypto
	return t
}

// LatestMessage 最新的消息记录
func (t *Topic) LatestMessage() *HistoryRecord {
	v := t.historyRecords.Right()
	r, ok := v.(*HistoryRecord)
	if !ok {
		return nil
	}
	return r
}

// AddForwardingAddr 添加转发器
func (t *Topic) AddForwardingAddr(topic *Topic) (int, ErrCode) {
	if bytes.Compare(t.Name, topic.Name) == 0 {
		return 0, ErrCodeExchangeSame
	}
	// TODO: 去重，不允许添加重复的
	t.forwardLock.Lock()
	defer t.forwardLock.Unlock()

	for i := 0; i < ForwardingAddrsMaxNum; i++ {
		if t.ForwardingAddrs[i] == nil {
			t.ForwardingAddrs[i] = topic

			// 更新最大坐标
			if t.ForwardingAddrsEnd < i+1 {
				t.ForwardingAddrsEnd = i + 1
			}

			return i, ErrCodeOK
		}
	}

	return 0, ErrCodeExchangeIsFull
}

// DelForwardingAddr 删除转发器
func (t *Topic) DelForwardingAddr(topic *Topic) (int, ErrCode) {
	t.forwardLock.Lock()
	defer t.forwardLock.Unlock()

	for i := 0; i < t.ForwardingAddrsEnd; i++ {
		if bytes.Compare(t.ForwardingAddrs[i].Name, topic.Name) == 0 {
			t.ForwardingAddrs[i] = nil
			break
		}
	}

	return 0, ErrCodeOK
}

func NewTopic(name []byte, bufferSize, historySize int) *Topic {
	t := &Topic{
		Name:        name,
		HistorySize: historySize,
		Offset:      0,
		counter:     proto.NewCounter(),
		consumers:   &sync.Map{},
		queue:       make(chan *proto.CMessage, bufferSize),
		crypto:      &proto.NoCrypto{},
		forwardLock: &sync.Mutex{},
		onConsumed:  func(_ *HistoryRecord) {},
	}
	go t.consume()

	return t
}
