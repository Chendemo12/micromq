package sdk

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Chendemo12/functools/helper"
	"github.com/Chendemo12/functools/httpc"
	"github.com/Chendemo12/micromq/src/proto"
)

// NewHttpProducer 创建一个HTTP的生产者
func NewHttpProducer(addr string) *HttpProducer {
	for strings.HasSuffix(addr, "/") {
		addr = addr[:len(addr)-1]
	}

	p := &HttpProducer{
		Addr:   addr,
		crypto: &proto.NoCrypto{},
	}
	p.SetPath("/api/edge/product").SetAsyncPath("/api/edge/product/async")

	return p
}

// ProducerForm 生产者消息投递表单, 不允许将多个消息编码成一个消息帧;
// token若为空则认为不加密;
// value是对加密后的消息体进行base64编码后的结果,依据token判断是否需要解密
type ProducerForm struct {
	Topic string `json:"topic" description:"消息主题"`
	Key   string `json:"key,omitempty" description:"消息键"`
	Value string `json:"value" description:"base64编码后的消息体"`
	Token string `json:"token,omitempty" description:"认证密钥"`
}

func (m ProducerForm) String() string {
	// "<ProducerForm> on [ T::DNS_UPDATE | K::2023-07-22T12:23:48.767 ] with 200 bytes of payload"
	return fmt.Sprintf(
		"<ProducerForm> on [ T::%s | K::%s ] with %d bytes of payload",
		m.Topic, m.Key, len(m.Value),
	)
}

func (m ProducerForm) IsEncrypt() bool { return m.Token != "" }

// ProductResponse 消息返回值; 仅当 status=Accepted 时才认为服务器接受了请求并正确的处理了消息
type ProductResponse struct {
	Status       string `json:"status" validate:"oneof=Accepted UnmarshalFailed TokenIncorrect Let-ReRegister Refused" description:"消息接收状态"`
	Offset       uint64 `json:"offset" description:"消息偏移量"`
	ResponseTime int64  `json:"response_time" description:"服务端返回消息时的时间戳"`
	Message      string `json:"message" description:"额外的消息描述"`
}

func (m ProductResponse) String() string {
	return fmt.Sprintf(
		"<ProductResponse> with status: %s | %d", m.Status, m.Offset,
	)
}

func (m ProductResponse) Error() error {
	switch m.Status {
	case "UnmarshalFailed":
		return errors.New("broker unmarshal message failed")
	case proto.GetMessageResponseStatusText(proto.RefusedStatus):
		return errors.New("broker refused message")
	case proto.GetMessageResponseStatusText(proto.TokenIncorrectStatus):
		return ErrTokenIncorrect
	default:
		return nil
	}
}

// IsOK 消息是否发送成功
func (m ProductResponse) IsOK() bool {
	return m.Status == proto.GetMessageResponseStatusText(proto.AcceptedStatus)
}

// HttpProducer HTTP 方式生产者
//
//	# Usage:
//
//		p := NewHttpProducer("http://127.0.0.1:1234")
//		p.SetToken("token")
//
//		resp, err := p.Send("topic", "key", []byte("value"))
//		if err != nil {
//			panic(err)
//		}
type HttpProducer struct {
	Addr      string `json:"addr,omitempty"`
	path      string
	asyncPath string
	token     string
	crypto    proto.Crypto
}

// Url 请求路由
func (p *HttpProducer) Url() string { return p.Addr + p.path }

// AsyncUrl 异步方法请求路由
func (p *HttpProducer) AsyncUrl() string { return p.Addr + p.asyncPath }

// SetPath 修改broker路径
func (p *HttpProducer) SetPath(path string) *HttpProducer {
	if !strings.HasPrefix(path, "/") {
		p.path = "/" + path
	} else {
		p.path = path
	}
	return p
}

// SetAsyncPath 修改broker异步方法路径
func (p *HttpProducer) SetAsyncPath(path string) *HttpProducer {
	if !strings.HasPrefix(path, "/") {
		p.asyncPath = "/" + path
	} else {
		p.asyncPath = path
	}
	return p
}

// SetToken 设置认证密钥
// @param token string 认证密钥,通常为原始密码的一次SHA256后的十六进制字符串 proto.CalcSHA
func (p *HttpProducer) SetToken(token string) *HttpProducer {
	p.crypto = &proto.TokenCrypto{Token: token}
	p.token = token

	return p
}

// SetPassword 设置认证密码
// @param password string 原始密码
func (p *HttpProducer) SetPassword(password string) *HttpProducer {
	token := proto.CalcSHA(password)
	p.crypto = &proto.TokenCrypto{Token: token}
	p.token = token

	return p
}

// Send 发送消息
func (p *HttpProducer) Send(topic, key string, value []byte) (*ProductResponse, error) {
	if topic == "" {
		return nil, ErrTopicEmpty
	}

	msg := &ProducerForm{}
	msg.Topic = topic
	msg.Key = key
	msg.Token = p.token

	var content []byte
	var err error

	// HTTP传输的数据必须进行加密
	// 加密消息体, 230914目前实际仅支持token一种加密方式
	if p.token != "" {
		content, err = p.crypto.Encrypt(value)
		if err != nil {
			return nil, err
		}
	} else {
		content = value[:]
	}
	// base64 编码
	msg.Value = helper.Base64Encode(content)

	// 发起HTTP请求
	return httpc.Post[*ProductResponse](p.Url(), nil, msg)
}

// Post 发送消息
func (p *HttpProducer) Post(topic, key string, form any) (*ProductResponse, error) {
	value, err := helper.JsonMarshal(form)
	if err != nil {
		return nil, err
	}

	return p.Send(topic, key, value)
}

// CreateSHA 计算字符串的SHA256值
func (p *HttpProducer) CreateSHA(str string) string {
	return proto.CalcSHA(str)
}
