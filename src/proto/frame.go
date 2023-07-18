package proto

import (
	"encoding/binary"
	"io"
)

// TransferFrame TCP传输协议帧, 可一次性传输多条消息
// TODO: 实现 Reader 和 Writer 接口
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

func (f *TransferFrame) MessageType() MessageType         { return ValidMessageType }
func (f *TransferFrame) MarshalMethod() MarshalMethodType { return BinaryMarshalMethod }

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

func (f *TransferFrame) ParseFrom(reader io.Reader) error {
	// TODO: 从流中解析消息帧
	return nil
}

func (f *TransferFrame) ParseTo(reader io.Reader) (Message, error) {
	// 将Data解析为具体的消息，返回指针
	var msg Message
	switch f.Type {
	case CMessageType:
		msg = &CMessage{}
	case PMessageType:
		msg = &PMessage{}
	case RegisterMessageType:
		msg = &RegisterMessage{}
	case RegisterMessageRespType:
		msg = &MessageResponse{}
	default:
		msg = &ValidMessage{}
	}

	err := msg.ParseFrom(reader)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

func (f *TransferFrame) Read(buf []byte) (int, error) {
	// TODO:
	return 0, nil
}

func (f *TransferFrame) Write(buf []byte) (int, error) {
	// TODO:
	return 0, nil
}

// BuildFields 构建缺省字段
func (f *TransferFrame) BuildFields() {
	binary.BigEndian.AppendUint16(f.DataSize, uint16(len(f.Data)))
	binary.BigEndian.AppendUint16(f.Checksum, CalcChecksum(f.Data))
}

// Build 编码消息帧
func (f *TransferFrame) Build() []byte {
	f.BuildFields()

	length := f.Length()
	content := make([]byte, length)
	content[0] = f.Head
	content[1] = byte(f.Type)
	content[2] = f.DataSize[0]
	content[3] = f.DataSize[1]

	copy(content[4:], f.Data)

	content[length-3] = f.Checksum[0]
	content[length-2] = f.Checksum[1]
	content[length-1] = f.Tail

	// TODO: TransferFrame 实现 io 接口
	return content
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

func (f *TransferFrame) WriteP(pms ...*PMessage) []byte {
	f.Data = BuildPMessages(pms...)
	return f.Build()
}

func (f *TransferFrame) WriteC(cms ...*CMessage) []byte {
	f.Data = BuildCMessages(cms...) // 必须先为 Data 赋值
	return f.Build()
}
