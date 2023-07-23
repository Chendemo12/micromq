package proto

func GetDescriptor(code MessageType) *Descriptor { return descriptors[code] }

// AddDescriptor 添加描述符号表, 对于已经实现的协议则不允许修改
func AddDescriptor(m Message, text string) bool {
	if descriptors[m.MessageType()].UserDefined() {
		descriptors[m.MessageType()].code = m.MessageType()
		descriptors[m.MessageType()].message = m
		descriptors[m.MessageType()].text = text
		return true
	}
	return false
}

type Descriptor struct {
	code        MessageType
	message     Message
	text        string
	userDefined bool
}

// MessageType 协议类别
func (m Descriptor) MessageType() MessageType { return m.code }

// Message 协议定义,可能为nil
func (m Descriptor) Message() Message { return m.message }

// Text 协议类别的文字描述
func (m Descriptor) Text() string { return m.text }

// UserDefined 是否是用户自定义协议
func (m Descriptor) UserDefined() bool { return m.userDefined }

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
		}
	}

	// 替换已经实现的协议
	descriptors[NotImplementMessageType].userDefined = false // 此协议不允许自定义

	descriptors[RegisterMessageType] = &Descriptor{
		code:        RegisterMessageType,
		message:     &RegisterMessage{},
		text:        "RegisterMessage",
		userDefined: false,
	}

	descriptors[RegisterMessageRespType] = &Descriptor{
		code:        RegisterMessageRespType,
		message:     &MessageResponse{Offset: 0}, // Offset == 0
		text:        "RegisterMessageResponse",
		userDefined: false,
	}

	descriptors[ReRegisterMessageType] = &Descriptor{
		code:        ReRegisterMessageType,
		message:     nil, // no payload
		text:        "Re-RegisterMessage",
		userDefined: false,
	}

	descriptors[MessageRespType] = &Descriptor{
		code:        MessageRespType,
		message:     &MessageResponse{Offset: 1}, // Offset != 0
		text:        "MessageResponse",
		userDefined: false,
	}

	descriptors[PMessageType] = &Descriptor{
		code:        PMessageType,
		message:     &PMessage{},
		text:        "ProducerMessage",
		userDefined: false,
	}

	descriptors[CMessageType] = &Descriptor{
		code:        CMessageType,
		message:     &CMessage{},
		text:        "ConsumerMessage",
		userDefined: false,
	}
}
