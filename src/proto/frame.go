package proto

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// TransferFrame 传输协议帧
//
// 帧与底层的传输协议无关, 帧是包含了具体消息的增加了冗余校验等信息的数据报;
// 帧内部可以包含一条或多条相同类型的消息, 因此帧可一次性传输多条相同的消息;
// 帧的大小没有限制, 真正对帧大小有限制的是底层的传输层, 具体是 transfer.Transfer 的要求;
//
//	帧结构：
//		|   head   |   mType  |   dataSize   |        data        |   checksum   |   tail   |
//		|----------|----------|--------------|--------------------|--------------|----------|
//	len	|     1	   |    1     |       2      |      N bytes       |       2      |     1    |
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
	head     byte        // 恒为 FrameHead
	mType    MessageType // data 包含的消息类型
	dataSize []byte      // 标识消息总长度,2个字节, data 的长度, 同样适用于多帧报文
	data     []byte      // 若干个消息
	checksum []byte      // checksum 经典校验和算法,2个字节, data 的校验和
	tail     byte        // 恒为 FrameTail
}

// 构建缺省字段
func (f *TransferFrame) buildFields() {
	binary.BigEndian.PutUint16(f.dataSize, uint16(len(f.data)))
	binary.BigEndian.PutUint16(f.checksum, CalcChecksum(f.data))
}

// 依据帧类型创建一个新的消息指针实例
func (f *TransferFrame) newMessage() (msg Message) {
	switch f.mType {
	case CMessageType:
		msg = &CMessage{PM: &PMessage{}}
	case PMessageType:
		msg = &PMessage{}
	case RegisterMessageType:
		msg = &RegisterMessage{}
	case RegisterMessageRespType, MessageRespType:
		msg = &MessageResponse{}
	case HeartbeatMessageType:
		msg = &HeartbeatMessage{}
	default:
		msg = &NotImplementMessage{}
	}
	return
}

func (f *TransferFrame) Head() byte { return FrameHead }

func (f *TransferFrame) Type() MessageType { return f.mType }

// DataSize 获得消息的总长度, 由 dataSize 标识
func (f *TransferFrame) DataSize() int {
	return int(binary.BigEndian.Uint16(f.dataSize))
}

func (f *TransferFrame) Tail() byte { return FrameTail }

func (f *TransferFrame) Payload() []byte { return f.data }

// Text 获取帧的文字描述
func (f *TransferFrame) Text() string {
	return fmt.Sprintf("<frame:%s>", descriptors[f.mType].text)
}

// MessageText 获取帧内消息的文字描述
func (f *TransferFrame) MessageText() string { return descriptors[f.mType].text }

// SetType 修改帧类型
func (f *TransferFrame) SetType(typ MessageType) *TransferFrame {
	f.mType = typ
	return f
}

// SetPayload 修改帧载荷信息
func (f *TransferFrame) SetPayload(data []byte) *TransferFrame {
	f.data = data[:]
	return f
}

// Checksum 获取帧校验和, 由 checksum 标识
func (f *TransferFrame) Checksum() uint16 {
	return binary.BigEndian.Uint16(f.checksum)
}

func (f *TransferFrame) MarshalMethod() MarshalMethodType {
	return BinaryMarshalMethod
}

func (f *TransferFrame) String() string {
	return fmt.Sprintf(
		"<frame:%s> [ CS::%d ] with %d bytes of payload",
		descriptors[f.mType].text, f.Checksum(), len(f.data),
	)
}

// Length 获得帧总长
func (f *TransferFrame) Length() int { return f.DataSize() + 7 }

func (f *TransferFrame) Reset() {
	f.head = FrameHead
	f.mType = NotImplementMessageType
	f.dataSize = make([]byte, 2)
	f.data = make([]byte, 0)
	f.checksum = make([]byte, 2)
	f.tail = FrameTail
}

// =================================== 帧的编解码 ==================================

// Build 编码消息帧用于显示获取构建好的字节流
func (f *TransferFrame) Build() []byte {
	f.buildFields()

	length := len(f.data) + 7
	content := make([]byte, length)
	content[0] = f.head
	content[1] = byte(f.mType)
	content[2] = f.dataSize[0]
	content[3] = f.dataSize[1]

	copy(content[4:len(f.data)+4], f.data)

	content[length-3] = f.checksum[0]
	content[length-2] = f.checksum[1]
	content[length-1] = f.tail

	return content
}

// BuildWith 补充字段,编码消息帧, 仅适用于构建包含单个消息的帧
// 若需要包含多个消息, 需使用 FrameCombine 方法
func (f *TransferFrame) BuildWith(typ MessageType, data []byte, encrypt ...EncryptFunc) error {
	f.mType = typ
	f.data = data[:]

	if len(encrypt) < 1 || !typ.EncryptionAllowed() { // 未开启加密，某些消息不允许加密传输
		return nil
	}

	// 开启加密
	_bytes, err := encrypt[0](f.data)
	if err != nil {
		return fmt.Errorf("message encrypt failed: %v", err)
	}
	f.data = _bytes[:]

	return nil
}

// BuildFrom 从协议中构建消息帧, 仅适用于构建包含单个消息的帧
// 若需要包含多个消息, 需使用 FrameCombine 方法
func (f *TransferFrame) BuildFrom(m Message, encrypt ...EncryptFunc) error {
	_bytes, err := m.build()
	if err != nil {
		return fmt.Errorf("message build failed: %v", err)
	}

	return f.BuildWith(m.MessageType(), _bytes, encrypt...)
}

// WriteTo 将预构建的消息流直接写入 io.Writer 内
// 相比于返回[]byte的 BuildWith 和 BuildFrom 方法而言,能减少一倍的内存分配
func (f *TransferFrame) WriteTo(writer io.Writer) (int64, error) {
	f.buildFields()
	var all = 0

	i, err := writer.Write([]byte{f.head})
	if err != nil {
		return int64(i), err
	}
	all += i

	i, err = writer.Write([]byte{byte(f.mType)})
	if err != nil {
		return int64(i), err
	}
	all += i

	i, err = writer.Write(f.dataSize)
	if err != nil {
		return int64(i), err
	}
	all += i

	i, err = writer.Write(f.data)
	if err != nil {
		return int64(i), err
	}
	all += i

	i, err = writer.Write(f.checksum)
	if err != nil {
		return int64(i), err
	}
	all += i

	i, err = writer.Write([]byte{f.tail})
	if err != nil {
		return int64(i), err
	}
	all += i

	return int64(all), nil
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
	f.mType = MessageType(bc.oneByte[0])

	bc.i, bc.err = reader.Read(f.dataSize)
	f.data = make([]byte, f.DataSize())

	bc.i, bc.err = reader.Read(f.data)
	if bc.err != nil {
		return bc.err
	}
	bc.i, bc.err = reader.Read(f.checksum)
	if bc.err != nil {
		return fmt.Errorf("checksum date read failed: %v", bc.err)
	}

	return nil
}

// =================================== 帧的编解码 End ==============================

// Unmarshal 反序列化帧消息体
func (f *TransferFrame) Unmarshal(msg Message, decrypt ...DecryptFunc) error {
	var err error
	if len(decrypt) > 0 && msg.MessageType().EncryptionAllowed() { // 消息允许加密
		f.data, err = decrypt[0](f.data)
		if err != nil {
			return err
		}
	}

	// 针对不同的解析类型选择最优的解析方法
	if msg.MarshalMethod() == BinaryMarshalMethod {
		err = msg.parseFrom(bytes.NewBuffer(f.data))
	} else {
		err = msg.parse(f.data)
	}

	return err
}

// UnmarshalTo 将帧消息解析成某一个具体的协议消息
// 此方法与 Unmarshal 的区别在于：Unmarshal 会显式的依据消息类型从流中解析数据
// 而 UnmarshalTo 则首先会依据 TransferFrame.mType 推断消息类型并创建一个新的实例,解析后返回
// 因此在调用 UnmarshalTo 之前不得修改 TransferFrame.mType 的值
func (f *TransferFrame) UnmarshalTo(decrypt ...DecryptFunc) (Message, error) {
	// 将Data解析为具体的消息，返回指针
	var msg = f.newMessage()

	// 解密并反序列化
	err := f.Unmarshal(msg, decrypt...)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

// FrameSplit 消息帧拆分，将消息帧内的数据解析提取为若干个消息
// 因此在调用此方法之前不得调用 TransferFrame.SetType
func FrameSplit[T Message](f *TransferFrame, msgs *[]T, decrypt ...DecryptFunc) error {
	var err error
	var msg Message

	// 首先解密整个消息体
	if len(decrypt) > 0 && f.mType.EncryptionAllowed() { // 消息允许加密
		f.data, err = decrypt[0](f.data)
		if err != nil { // 帧解密失败
			return err
		}
	}

	stream := bytes.NewReader(f.data)

	if f.mType.CombinationAllowed() { // 允许组合，循环解析消息
		for err == nil && stream.Len() > 0 {
			msg = f.newMessage()
			err = msg.parseFrom(stream)
			if err == nil {
				*msgs = append(*msgs, msg.(T))
			}
		}
	} else { // 此类型不允许组合，因此帧内只包含一个消息
		msg = f.newMessage()
		err = msg.parseFrom(stream)
		if err == nil {
			*msgs = append(*msgs, msg.(T))
		}
	}

	return err
}

// FrameCombine 组合消息帧，将若干个消息，组合到一个帧内
// 在调用此方法之前需首先设置帧类型 TransferFrame.SetType
// 组合空消息帧无意义, 因此需自行保证 msgs 不为空
func FrameCombine[T Message](f *TransferFrame, msgs []T, encrypt ...EncryptFunc) error {
	_ = msgs[0]

	var err error
	var _bytes []byte
	var end = len(msgs)

	if !f.mType.CombinationAllowed() { // 此类型不允许组合，因此帧内只包含一个消息
		end = 1
	}

	for i := 0; i < end; i++ {
		_bytes, err = msgs[i].build()
		if err != nil {
			return err
		}
		f.data = append(f.data, _bytes...)
	}

	if len(encrypt) > 0 && f.mType.EncryptionAllowed() {
		_bytes, err = encrypt[0](f.data)
		if err != nil {
			return err
		}
		f.data = _bytes[:]
	}

	return nil
}
