package proto

func GetDescriptor(typ MessageType) *Descriptor { return descriptors[typ] }

// AddDescriptor 添加描述符号表, 对于已经实现的协议则不允许修改
func AddDescriptor(m Message, ack Message, text string) bool {
	if descriptors[m.MessageType()].UserDefined() {
		descriptors[m.MessageType()].code = m.MessageType()
		descriptors[m.MessageType()].message = m
		descriptors[m.MessageType()].text = text
		descriptors[m.MessageType()].ackMessage = ack
		return true
	}
	return false
}

// IsMessageDefined 此消息是否可以被自由定义
func IsMessageDefined(typ MessageType) bool {
	return descriptors[typ].userDefined
}

type Descriptor struct {
	code        MessageType
	message     Message // 消息定义,可能为nil, 用于文档描述，无实际作用
	text        string  // 消息的文字描述
	userDefined bool    // 是否允许自定义
	ackMessage  Message // 消息交互的返回值 FIN-ACK
}

// MessageType 协议类别
func (m Descriptor) MessageType() MessageType { return m.code }

// Text 协议类别的文字描述
func (m Descriptor) Text() string { return m.text }

// UserDefined 是否是用户自定义协议
func (m Descriptor) UserDefined() bool { return m.userDefined }

// NeedACK 消息交互是否需要有返回值
func (m Descriptor) NeedACK() bool { return m.ackMessage != nil }

// 全局协议描述符表
//
//goland:noinspection GoUnusedGlobalVariable
var descriptors [TotalNumberOfMessages]*Descriptor

func init() {
	// 将所有协议全部初始化为 未实现的自定义协议
	for i := 0; i < TotalNumberOfMessages; i++ {
		descriptors[i] = &Descriptor{
			code:        NotImplementMessageType,
			message:     &NotImplementMessage{},
			text:        "NotImplementMessage",
			userDefined: true,
			ackMessage:  nil, // 不需要响应
		}
	}

	// 替换已经实现的协议
	descriptors[NotImplementMessageType].userDefined = false // 此协议不允许自定义

	descriptors[RegisterMessageType] = &Descriptor{
		code:        RegisterMessageType,
		message:     &RegisterMessage{},
		text:        "RegisterMessage",
		userDefined: false,
		ackMessage:  &MessageResponse{},
	}

	descriptors[RegisterMessageRespType] = &Descriptor{
		code:        RegisterMessageRespType,
		message:     &MessageResponse{Offset: 0}, // Offset == 0
		text:        "RegisterMessageResponse",
		userDefined: false,
	}

	descriptors[MessageRespType] = &Descriptor{
		code:        MessageRespType,
		message:     &MessageResponse{Offset: 1}, // Offset != 0
		text:        "MessageResponse",
		userDefined: false,
		ackMessage:  nil, // 不需要响应
	}

	descriptors[PMessageType] = &Descriptor{
		code:        PMessageType,
		message:     &PMessage{},
		text:        "ProducerMessage",
		userDefined: false,
		ackMessage:  &MessageResponse{}, // 需要给个确认消息
	}

	descriptors[CMessageType] = &Descriptor{
		code:        CMessageType,
		message:     &CMessage{},
		text:        "ConsumerMessage",
		userDefined: false,
		ackMessage:  &MessageResponse{}, // 需要给个确认消息
	}

	descriptors[HeartbeatMessageType] = &Descriptor{
		code:        HeartbeatMessageType,
		message:     &HeartbeatMessage{},
		text:        "HeartbeatMessage",
		userDefined: false,
	}
}
