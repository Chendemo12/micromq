package proto

import (
	"container/list"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"github.com/Chendemo12/fastapi-tool/helper"
	"io"
	"sync"
	"sync/atomic"
)

// CalcSHA 计算一个字符串的hash值
var CalcSHA = CalcSHA1

type NoCopy struct{}

func (*NoCopy) Lock()   {}
func (*NoCopy) Unlock() {}

// CalcChecksum 经典校验和算法
func CalcChecksum(data []byte) uint16 {
	sum := 0
	for i := 0; i < len(data); i += 2 {
		if i+1 == len(data) {
			sum += int(data[i])
		} else {
			sum += int(data[i])<<8 + int(data[i+1])
		}
	}
	sum = (sum >> 16) + (sum & 0xffff)
	sum += sum >> 16

	return uint16(^sum)
}

// NewCounter 创建一个新的计数器
func NewCounter() *Counter { return &Counter{v: 0, counter: &atomic.Uint64{}} }

// Counter 计数器
type Counter struct {
	v       uint64 // 必要时存储上一个值
	counter *atomic.Uint64
}

// Value 获取当前计数器的数值
func (c *Counter) Value() uint64 { return c.counter.Load() }

// Increment 计数器 +1，并返回新的值
func (c *Counter) Increment() {
	// 原子地将给定的增量添加到atomic.Uint64的值，并返回新的值
	c.counter.Add(1)
}

// ValueBeforeIncrement 首先获取当前计数器的数值，然后将计数器 +1
func (c *Counter) ValueBeforeIncrement() uint64 {
	c.v = c.counter.Load()
	c.counter.Add(1)
	return c.v
}

func NewQueue(capacity int) *Queue {
	return &Queue{
		list:     list.New(),
		capacity: capacity,
		mu:       &sync.Mutex{},
	}
}

type Queue struct {
	list     *list.List
	capacity int
	mu       *sync.Mutex
}

func (q *Queue) Capacity() int { return q.capacity }

func (q *Queue) Length() int { return q.list.Len() }

func (q *Queue) Append(value any) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.list.Len() >= q.capacity {
		q.list.Remove(q.list.Front())
	}
	q.list.PushBack(value)
}

func (q *Queue) PopLeft() any {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.list.Len() == 0 {
		return nil
	}

	element := q.list.Front()
	q.list.Remove(element)
	return element.Value
}

// CalcSHA256 计算字符串的哈希值
func CalcSHA256(str string) string {
	if str == "" {
		return ""
	}

	h := sha256.New()
	h.Write([]byte(str))
	hashValue := h.Sum(nil)

	// 将哈希值转换为16进制字符串输出
	return hex.EncodeToString(hashValue)
}

// CalcSHA1 计算字符串的哈希值
func CalcSHA1(str string) string {
	if str == "" {
		return ""
	}

	h := sha1.New()
	h.Write([]byte(str))
	hashValue := h.Sum(nil)

	return hex.EncodeToString(hashValue)
}

// ----------------------------------------------------------------------------

func JsonMessageBuild(m Message) ([]byte, error) {
	if m.MarshalMethod() == JsonMarshalMethod {
		return helper.JsonMarshal(m)
	}
	return m.Build()
}

func JsonMessageBuildTo(writer io.Writer, m Message) (int, error) {
	_bytes, err := m.Build()
	if err != nil {
		return 0, err
	}
	return writer.Write(_bytes)
}

func JsonMessageParse(stream []byte, m Message) error {
	return helper.JsonUnmarshal(stream, m)
}

// JsonMessageParseFrom 从reader解析消息，此操作不够优化，应考虑使用 Parse 方法
func JsonMessageParseFrom(reader io.Reader, m Message) error {
	_bytes := make([]byte, 65526)
	n, err := reader.Read(_bytes)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}

	return helper.JsonUnmarshal(_bytes[:n], m)
}

// ----------------------------------------------------------------------------
