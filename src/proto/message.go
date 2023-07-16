// Package proto 若涉及到字节序,则全部为大端序
package proto

import (
	"bytes"
	"encoding/binary"
	"io"
	"time"
)

// PMessage 生产者消息数据, 不允许复制
type PMessage struct {
	noCopy NoCopy
	Topic  []byte // 字符串转字节
	Key    []byte
	Value  []byte
}

func (m *PMessage) Length() int { return len(m.Topic) + len(m.Key) + len(m.Value) }

func (m *PMessage) Reset() {
	m.Topic = nil
	m.Key = nil
	m.Value = nil
}

// CMessage 消费者消息记录, 不允许复制
type CMessage struct {
	Offset      []byte // uint64
	ProductTime []byte // time.Time.Unix() 消息创建的Unix时间戳
	Pm          *PMessage
}

func (m *CMessage) Length() int { return 16 + m.Pm.Length() }

func (m *CMessage) Reset() {
	m.Offset = make([]byte, 8)
	m.ProductTime = make([]byte, 8)
	m.Pm.Reset()
}

type MessageResponse struct {
	noCopy      NoCopy
	Result      bool      `json:"result"`
	Offset      uint64    `json:"offset,omitempty"`
	ReceiveTime time.Time `json:"receive_time,omitempty"`
}

func (m *MessageResponse) Reset() {
	m.Result = false
	m.Offset = 0
}

// RegisterMessage 消息注册,适用于生产者和消费者
type RegisterMessage struct {
	Topics []string `json:"topics"` // 对于生产者,无意义
	Ack    AckType  `json:"ack"`
	Type   LinkType `json:"type"`
}

// TransferFrame TCP传输协议帧, 可一次性传输多条消息
type TransferFrame struct {
	Head     byte        // 恒为 FrameHead
	Type     MessageType // Data 包含的消息类型
	DataSize []byte      // 标识消息总长度,2个字节, Data 的长度, 同样适用于多帧报文
	Data     []byte      // 若干个消息
	Checksum []byte      // Checksum 经典校验和算法,2个字节, Data 的校验和 TODO: 修改为[Type, Data] 的校验和
	Tail     byte        // 恒为 FrameTail
}

// TransferFrame.Data 字节序列说明, 无作用
type container struct {
	pms []struct {
		TopicLength byte
		Topic       []byte
		KeyLength   byte
		Key         []byte
		ValueLength uint16
		Value       []byte
	}
	cms []struct {
		TopicLength byte
		Topic       []byte
		KeyLength   byte
		Key         []byte
		ValueLength uint16
		Value       []byte
		Offset      uint64
		ProductTime int64 // time.Time.Unix()
	}
}

// Length 获得帧总长
func (f *TransferFrame) Length() int { return len(f.Data) + 7 }

func (f *TransferFrame) Reset() {
	f.Head = FrameHead
	f.Type = ValidMessageType
	f.DataSize = make([]byte, 2)
	f.Data = nil
	f.Checksum = make([]byte, 2)
	f.Tail = FrameTail
}

func (f *TransferFrame) Read(buf []byte) (int, error) {
	// TODO:
	return 0, nil
}

func (f *TransferFrame) Write(buf []byte) (int, error) {
	// TODO:
	return 0, nil
}

func (f *TransferFrame) buildFields() {
	binary.BigEndian.AppendUint16(f.DataSize, uint16(len(f.Data)))
	binary.BigEndian.AppendUint16(f.Checksum, CalcChecksum(f.Data))
}

func (f *TransferFrame) write(writer io.Writer) (i int, err error) {
	n, err := writer.Write([]byte{FrameHead})
	n, err = writer.Write([]byte{byte(f.Type)})
	n, err = writer.Write(f.DataSize)
	n, err = writer.Write(f.Data)
	n, err = writer.Write(f.Checksum)

	if err != nil {
		return n, err
	}

	// 写入结束符
	return writer.Write([]byte{FrameTail})
}

func (f *TransferFrame) WriteP(pms ...*PMessage) *bytes.Buffer {
	f.Data = BuildPMessages(pms...)
	f.buildFields()

	content := make([]byte, f.Length())
	stream := bytes.NewBuffer(content)

	// TODO: TransferFrame 实现 io 接口
	return stream
}

func (f *TransferFrame) WriteC(cms ...*CMessage) *bytes.Buffer {
	f.Data = BuildCMessages(cms...) // 必须先为 Data 赋值
	f.buildFields()

	content := make([]byte, f.Length())
	stream := bytes.NewBuffer(content)

	// TODO: TransferFrame 实现 io 接口
	return stream
}
