package proto

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// TransferFrame TCP传输协议帧, 可一次性传输多条消息
//
//	帧结构：
//		|   Head   |   Type   |   DataSize   |        Data        |   Checksum   |   Tail   |
//		|----------|----------|--------------|--------------------|--------------|----------|
//	len	|    1     |    1     |       2      |  N [0-65526] bytes |       2      |     1    |
//	   	|----------|----------|--------------|--------------------|--------------|----------|
//	取值	|   0x3C   |          |              |                    |              |   0x0D   |
//		|----------|----------|--------------|--------------------|--------------|----------|
//
//	# Usage：
//
//	将帧载荷解析成具体的协议：
//		frame := TransferFrame{}
//		frame.Unmarshal(X)
//		X := frame.UnmarshalTo()
//
//	从消息构建帧：
//		frame := TransferFrame{}
//		frame.BuildWith(proto.MessageType, []byte{})
//		frame.BuildFrom(X)
type TransferFrame struct {
	counter  uint64      // 用以追踪此对象的实例是否由池创建
	Head     byte        // 恒为 FrameHead
	Type     MessageType // Data 包含的消息类型
	DataSize []byte      // 标识消息总长度,2个字节, Data 的长度, 同样适用于多帧报文
	Data     []byte      // 若干个消息
	Checksum []byte      // Checksum 经典校验和算法,2个字节, Data 的校验和
	Tail     byte        // 恒为 FrameTail
}

func (f *TransferFrame) MarshalMethod() MarshalMethodType {
	return BinaryMarshalMethod
}

func (f *TransferFrame) String() string {
	return fmt.Sprintf(
		"<frame:%s> [ CS::%d ] with %d bytes of payload",
		GetDescriptor(f.Type).Text(), f.ParseChecksum(), len(f.Data),
	)
}

// Parse 从字节序中解析数据帧
func (f *TransferFrame) Parse(stream []byte) error {
	return f.ParseFrom(bytes.NewReader(stream))
}

// ParseFrom 从流中解析数据帧
func (f *TransferFrame) ParseFrom(reader io.Reader) error {
	bc := bcPool.Get()
	defer bcPool.Put(bc)

	// 此处一定 > 7个字节
	bc.i, bc.err = reader.Read(bc.oneByte)
	bc.i, bc.err = reader.Read(bc.oneByte)
	f.Type = MessageType(bc.oneByte[0])

	bc.i, bc.err = reader.Read(f.DataSize)
	f.Data = make([]byte, f.ParseDataLength())

	bc.i, bc.err = reader.Read(f.Data)
	if bc.err != nil {
		return bc.err
	}
	bc.i, bc.err = reader.Read(f.Checksum)
	if bc.err != nil {
		return fmt.Errorf("checksum date read failed: %v", bc.err)
	}

	return nil
}

// Length 获得帧总长
func (f *TransferFrame) Length() int { return len(f.Data) + 7 }

func (f *TransferFrame) Reset() {
	f.Head = FrameHead
	f.Type = NotImplementMessageType
	f.DataSize = make([]byte, 2)
	f.Data = make([]byte, 0)
	f.Checksum = make([]byte, 2)
	f.Tail = FrameTail
}

// ParseDataLength 获得消息的总长度, DataSize 由标识
func (f *TransferFrame) ParseDataLength() int {
	return int(binary.BigEndian.Uint16(f.DataSize))
}

// ParseChecksum 将校验和转换为uint16类型
func (f *TransferFrame) ParseChecksum() uint16 {
	return binary.BigEndian.Uint16(f.Checksum)
}

// Unmarshal 反序列化帧消息体
func (f *TransferFrame) Unmarshal(msg Message, decrypt ...DecryptFunc) error {
	var err error
	if len(decrypt) > 0 {
		f.Data, err = decrypt[0](f.Data)
		if err != nil {
			return fmt.Errorf("message decrypt failed: %v", err)
		}
	}

	// 针对不同的解析类型选择最优的解析方法
	if msg.MarshalMethod() == BinaryMarshalMethod {
		err = msg.ParseFrom(bytes.NewBuffer(f.Data))
	} else {
		err = msg.Parse(f.Data)
	}

	return err
}

// UnmarshalTo 将帧消息解析成某一个具体的协议消息
func (f *TransferFrame) UnmarshalTo(decrypt ...DecryptFunc) (Message, error) {
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
		msg = &NotImplementMessage{}
	}

	// 解密并反序列化
	err := f.Unmarshal(msg, decrypt...)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

// 构建缺省字段
func (f *TransferFrame) buildFields() {
	binary.BigEndian.PutUint16(f.DataSize, uint16(len(f.Data)))
	binary.BigEndian.PutUint16(f.Checksum, CalcChecksum(f.Data))
}

// BuildWith 补充字段,编码消息帧
func (f *TransferFrame) BuildWith(typ MessageType, data []byte, encrypt ...EncryptFunc) ([]byte, error) {
	f.Type = typ

	if len(encrypt) < 1 {
		f.Data = data
		return f.Build()
	}

	// 开启加密
	_bytes, err := encrypt[0](data)
	if err != nil {
		return nil, fmt.Errorf("message encrypt failed: %v", err)
	}
	f.Data = _bytes

	return f.Build()
}

// BuildFrom 从协议中构建消息帧
func (f *TransferFrame) BuildFrom(m Message, encrypt ...EncryptFunc) ([]byte, error) {
	_bytes, err := m.Build()
	if err != nil {
		return nil, err
	}

	return f.BuildWith(m.MessageType(), _bytes, encrypt...)
}

// Build 编码消息帧 (最终方法)
func (f *TransferFrame) Build() ([]byte, error) {
	f.buildFields()

	length := f.Length()
	content := make([]byte, length)
	content[0] = f.Head
	content[1] = byte(f.Type)
	content[2] = f.DataSize[0]
	content[3] = f.DataSize[1]

	copy(content[4:len(f.Data)+4], f.Data)

	content[length-3] = f.Checksum[0]
	content[length-2] = f.Checksum[1]
	content[length-1] = f.Tail

	return content, nil
}

// TODO: io.Reader 接口实现
func (f *TransferFrame) Read(buf []byte) (int, error) {
	return 0, nil
}

// io.Writer 接口实现
func (f *TransferFrame) Write(buf []byte) (int, error) {
	return 0, nil
}
