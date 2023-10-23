package mq

import (
	"fmt"
	"github.com/Chendemo12/fastapi"
	"github.com/Chendemo12/fastapi-tool/helper"
	"github.com/Chendemo12/micromq/src/proto"
	"time"
)

type opt = fastapi.Opt

// EdgeRouter edge路由组
func EdgeRouter() *fastapi.Router {
	var router = fastapi.APIRouter("/api/edge", []string{"EDGE"})

	{
		router.Post("/product", PostProducerMessage, opt{
			Summary:       "发送一个生产者消息",
			Description:   "阻塞式发送生产者消息，此接口会在消息成功发送给消费者后返回",
			RequestModel:  &ProducerForm{},
			ResponseModel: &ProductResponse{},
		})

		router.Post("/product/async", AsyncPostProducerMessage, opt{
			Summary:       "异步发送一个生产者消息",
			Description:   "非阻塞式发送生产者消息，服务端会在消息解析成功后立刻返回结果，不保证消息已发送给消费者",
			RequestModel:  &ProducerForm{},
			ResponseModel: &ProductResponse{},
		})
	}

	return router
}

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

func toPMessage(c *fastapi.Context) (*proto.PMessage, *fastapi.Response) {
	form := &ProducerForm{}
	resp := c.ShouldBindJSON(form)
	if resp != nil {
		return nil, resp
	}

	c.Logger().Debug(fmt.Sprintf("receive: %s, from '%s' ", form, c.EngineCtx().IP()))
	pm := &proto.PMessage{}
	// 首先反序列化消息体
	decode, err := helper.Base64Decode(form.Value)
	if err != nil {
		c.Logger().Info("message UnmarshalFailed about:", c.EngineCtx().IP())
		c.Logger().Info(err)
		return nil, c.OKResponse(&ProductResponse{
			Status:       "UnmarshalFailed",
			Offset:       0,
			ResponseTime: time.Now().Unix(),
			Message:      err.Error(),
		})
	}

	// 解密消息
	if !form.IsEncrypt() {
		pm.Value = decode
	} else {
		if !mq.broker.IsTokenCorrect(form.Token) {
			c.Logger().Info(c.EngineCtx().IP(), "has wrong token")
			return nil, c.OKResponse(&ProductResponse{
				Status:       proto.GetMessageResponseStatusText(proto.TokenIncorrectStatus),
				Offset:       0,
				ResponseTime: time.Now().Unix(),
				Message:      err.Error(),
			})
		} else {
			// 如果设置了密钥，HTTP传输的数据必须进行加密
			_bytes, _err := mq.broker.Crypto().Decrypt(decode)
			if _err != nil {
				c.Logger().Warn(c.EngineCtx().IP(), "has correct token, but value decrypt failed: ", _err)
				return nil, c.OKResponse(&ProductResponse{
					Status:       proto.GetMessageResponseStatusText(proto.TokenIncorrectStatus),
					Offset:       0,
					ResponseTime: time.Now().Unix(),
					Message:      _err.Error(),
				})
			}
			pm.Value = _bytes
		}
	}

	pm.Key = []byte(form.Key)
	pm.Topic = []byte(form.Topic)

	return pm, nil
}

// PostProducerMessage 同步发送消息
func PostProducerMessage(c *fastapi.Context) *fastapi.Response {
	pm, resp := toPMessage(c)
	if resp != nil {
		return resp
	}

	respForm := &ProductResponse{}
	respForm.Status = proto.GetMessageResponseStatusText(proto.AcceptedStatus)
	respForm.Offset = mq.broker.Publisher(pm)
	respForm.ResponseTime = time.Now().Unix()

	c.Logger().Debug(fmt.Sprintf("return: %s, to '%s' ", respForm, c.EngineCtx().IP()))

	return c.OKResponse(respForm)
}

// AsyncPostProducerMessage 异步生产消息
func AsyncPostProducerMessage(c *fastapi.Context) *fastapi.Response {
	return PostProducerMessage(c)
}
