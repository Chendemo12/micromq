package mq

import (
	"fmt"
	"time"

	"github.com/Chendemo12/fastapi"
	"github.com/Chendemo12/functools/helper"
	logger "github.com/Chendemo12/functools/zaplog"
	"github.com/Chendemo12/micromq/src/proto"
)

type ProducerForm struct {
	fastapi.BaseModel
	Token string `json:"token,omitempty" description:"认证密钥"`
	Topic string `json:"topic" description:"消息主题"`
	Key   string `json:"key" description:"消息键"`
	Value string `json:"value" description:"base64编码后的消息体"`
}

func (m *ProducerForm) String() string {
	// "<ProducerForm> on [ T::DNS_UPDATE | K::2023-07-22T12:23:48.767 ] with 200 bytes of payload"
	return fmt.Sprintf(
		"<ProducerForm> on [ T::%s | K::%s ] with %d bytes of payload",
		m.Topic, m.Key, len(m.Value),
	)
}

func (m *ProducerForm) SchemaDesc() string {
	return `生产者消息投递表单, 不允许将多个消息编码成一个消息帧; 
token若为空则认为不加密; 
value是对加密后的消息体进行base64编码后的结果,依据token判断是否需要解密`
}

func (m *ProducerForm) IsEncrypt() bool { return m.Token != "" }

type ProductResponse struct {
	fastapi.BaseModel
	// 仅当 Accepted 时才认为服务器接受了请求并下方了有效的参数
	Status       string `json:"status" validate:"oneof=Accepted UnmarshalFailed TokenIncorrect Let-ReRegister Refused" description:"消息接收状态"`
	Offset       uint64 `json:"offset" description:"消息偏移量"`
	ResponseTime int64  `json:"response_time" description:"服务端返回消息时的时间戳"`
	Message      string `json:"message" description:"额外的消息描述"`
}

func (m *ProductResponse) String() string {
	return fmt.Sprintf(
		"<ProductResponse> with status: %s | %d", m.Status, m.Offset,
	)
}

func (m *ProductResponse) SchemaDesc() string {
	return "消息返回值; 仅当 status=Accepted 时才认为服务器接受了请求并正确的处理了消息"
}

// EdgeRouter edge路由组
type EdgeRouter struct {
	fastapi.BaseGroupRouter
}

func (r *EdgeRouter) Prefix() string {
	return "/api/edge"
}

func (r *EdgeRouter) Tags() []string {
	return []string{"EDGE"}
}

func (r *EdgeRouter) Summary() map[string]string {
	return map[string]string{
		"PostProduct":      "发送一个生产者消息",
		"PostProductAsync": "异步发送一个生产者消息",
	}
}

func (r *EdgeRouter) Description() map[string]string {
	return map[string]string{
		"PostProduct":      "阻塞式发送生产者消息，此接口会在消息成功发送给消费者后返回",
		"PostProductAsync": "非阻塞式发送生产者消息，服务端会在消息解析成功后立刻返回结果，不保证消息已发送给消费者",
	}
}

func (r *EdgeRouter) PostProduct(c *fastapi.Context, param *ProducerForm) (*ProductResponse, error) {
	logger.Debugf("receive message form: %s", c.MuxContext().ClientIP())

	// 首先反序列化消息体
	decode, err := helper.Base64Decode(param.Value)
	if err != nil {
		//logger.Errorf("message UnmarshalFailed about: %s, err: %v", c.MuxContext().ClientIP(), err)
		return nil, err
	}

	pm := &proto.PMessage{}
	pm.Key = []byte(param.Key)
	pm.Topic = []byte(param.Topic)
	pm.Value = decode

	if param.IsEncrypt() { // 需要解密消息
		if !mq.broker.IsTokenCorrect(param.Token) {
			//logger.Debug(c.MuxContext().ClientIP() + " has wrong token")
			return &ProductResponse{
				Status:       proto.GetMessageResponseStatusText(proto.TokenIncorrectStatus),
				Offset:       0,
				ResponseTime: time.Now().Unix(),
				Message:      "wrong token",
			}, nil
		}

		// 如果设置了密钥，HTTP传输的数据必须进行加密
		_bytes, err := mq.broker.Crypto().Decrypt(decode)
		if err != nil {
			//logger.Warn(c.MuxContext().ClientIP()+" has correct token, but value decrypt failed: ", err)
			return &ProductResponse{
				Status:       proto.GetMessageResponseStatusText(proto.TokenIncorrectStatus),
				Offset:       0,
				ResponseTime: time.Now().Unix(),
				Message:      err.Error(),
			}, nil
		}

		pm.Value = _bytes
	}

	respForm := &ProductResponse{}
	respForm.Status = proto.GetMessageResponseStatusText(proto.AcceptedStatus)
	respForm.Offset = mq.broker.Publisher(pm)
	respForm.ResponseTime = time.Now().Unix()

	logger.Debugf("return: %s, to '%s' ", respForm, c.MuxContext().ClientIP())

	return respForm, nil
}

func (r *EdgeRouter) PostProductAsync(c *fastapi.Context, param *ProducerForm) (*ProductResponse, error) {
	return r.PostProduct(c, param)
}
