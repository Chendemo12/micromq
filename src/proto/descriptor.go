package proto

// Descriptors 全局协议描述符
func Descriptors() []*Descriptor { return descriptors }

type Descriptor struct {
	kind        MessageType
	message     Message
	text        string
	userDefined bool
}

// MessageType 协议类别
func (m Descriptor) MessageType() MessageType { return m.kind }

// Message 协议定义,可能为nil
func (m Descriptor) Message() Message { return m.message }

// Text 协议类别的文字描述
func (m Descriptor) Text() string { return m.text }

// UserDefined 是否是用户自定义协议
func (m Descriptor) UserDefined() bool { return m.userDefined }

//goland:noinspection GoUnusedGlobalVariable
var descriptors = make([]*Descriptor, 256)

func init() {
	descriptors[NotImplementMessageType] = &Descriptor{
		kind:        NotImplementMessageType,
		message:     &NotImplementMessage{},
		text:        "NotImplementMessage",
		userDefined: false,
	}

	descriptors[RegisterMessageType] = &Descriptor{
		kind:        RegisterMessageType,
		message:     &RegisterMessage{},
		text:        "RegisterMessage",
		userDefined: false,
	}

	descriptors[RegisterMessageRespType] = &Descriptor{
		kind:        RegisterMessageRespType,
		message:     &MessageResponse{Offset: 0}, // Offset == 0
		text:        "RegisterMessageResponse",
		userDefined: false,
	}

	descriptors[ReRegisterMessageType] = &Descriptor{
		kind:        ReRegisterMessageType,
		message:     nil, // no payload
		text:        "Re-RegisterMessage",
		userDefined: false,
	}

	descriptors[MessageRespType] = &Descriptor{
		kind:        MessageRespType,
		message:     &MessageResponse{Offset: 1}, // Offset != 0
		text:        "MessageResponse",
		userDefined: false,
	}

	descriptors[PMessageType] = &Descriptor{
		kind:        PMessageType,
		message:     &PMessage{},
		text:        "ProducerMessage",
		userDefined: false,
	}

	descriptors[CMessageType] = &Descriptor{
		kind:        CMessageType,
		message:     &CMessage{},
		text:        "ConsumerMessage",
		userDefined: false,
	}
}
