package mq

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Chendemo12/fastapi"
	"github.com/Chendemo12/fastapi/middleware/fiberWrapper"
	"github.com/Chendemo12/fastapi/middleware/routers"
	"github.com/Chendemo12/fastapi/openapi"
	"github.com/Chendemo12/fastapi/pathschema"
	"github.com/Chendemo12/functools/helper"
	"github.com/Chendemo12/functools/python"
	"github.com/Chendemo12/micromq/src/engine"
	"github.com/Chendemo12/micromq/src/proto"
)

var pathSchema = pathschema.NewComposition(&pathschema.Backslash{}, &pathschema.LowerCase{})

func CreateEdge(conf *Config) *fastapi.Wrapper {
	mux := fastapi.New(fastapi.Config{
		Title:                 conf.AppName,
		Version:               conf.Version,
		Description:           conf.AppName + " Api Service",
		ShutdownTimeout:       5,
		DisableSwagAutoCreate: !python.Any(!conf.StatisticDisabled, conf.Debug),
	})

	eng := fiberWrapper.Default()
	eng.App().Use(fiberWrapper.DefaultCORS)
	eng.App().Use(NewAuthInterceptor(excludeRoutes, AuthInterceptor))

	mux.SetMux(eng)
	mux.UseBeforeWrite(ErrorLog)

	mux.IncludeRouter(routers.NewInfoRouter(mux.Config(), "/api/base"))
	mux.IncludeRouter(&ExchangeRouter{})
	mux.SetRouteErrorFormatter(ErrorFormatter)

	if python.Any(conf.EdgeEnabled, conf.Debug) {
		mux.IncludeRouter(&EdgeRouter{})
	}

	if python.Any(!conf.StatisticDisabled, conf.Debug) {
		mux.IncludeRouter(&StatRouter{})
	}
	return mux
}

type ConsumerStatistic struct {
	Addr   string   `json:"addr" description:"连接地址"`
	Topics []string `json:"topics" description:"订阅的主题名列表"`
}

func (m *ConsumerStatistic) SchemaDesc() string {
	return "消费者统计信息"
}

type TopicOffsetStatistic struct {
	Topic  string `json:"topic" description:"名称"`
	Offset uint64 `json:"offset" description:"最新的消息偏移量"`
}

func (m *TopicOffsetStatistic) SchemaDesc() string {
	return "Topic的消息偏移量信息"
}

type TopicRecordStatistic struct {
	Topic       string `json:"topic" description:"名称"`
	Key         string `json:"key"`
	Value       string `json:"value" description:"base64编码后的消息体明文"`
	Offset      uint64 `json:"offset" description:"消息偏移量"`
	ProductTime int64  `json:"product_time" description:"消息接收时间戳"`
}

type TopicConsumerStatistic struct {
	Topic     string   `json:"topic" description:"名称"`
	Consumers []string `json:"consumers" description:"消费者连接"`
}

type ProducerForm struct {
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

// ====

type EdgeRouter struct {
	fastapi.BaseGroupRouter
}

func (r *EdgeRouter) Prefix() string { return "/api/edge" }

func (r *EdgeRouter) PathSchema() pathschema.RoutePathSchema {
	return pathSchema
}

func (r *EdgeRouter) Path() map[string]string {
	return map[string]string{}
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

// PostProduct 发送一个生产者消息
func (r *EdgeRouter) PostProduct(c *fastapi.Context, form *ProducerForm) (*ProductResponse, error) {
	clientIp := c.MuxContext().ClientIP()

	mq.Logger().Debug(fmt.Sprintf("receive: %s, from '%s' ", form, clientIp))

	pm := &proto.PMessage{}
	// 首先反序列化消息体
	decode, err := helper.Base64Decode(form.Value)
	if err != nil {
		mq.Logger().Info("message UnmarshalFailed about:", clientIp)
		mq.Logger().Info(err)
		return &ProductResponse{
			Status:       "UnmarshalFailed",
			Offset:       0,
			ResponseTime: time.Now().Unix(),
			Message:      err.Error(),
		}, nil
	}

	pm.Value = decode
	// 解密消息
	if form.IsEncrypt() {
		if !mq.broker.IsTokenCorrect(form.Token) {
			mq.Logger().Info(clientIp, "has wrong token")
			return &ProductResponse{
				Status:       proto.GetMessageResponseStatusText(proto.TokenIncorrectStatus),
				Offset:       0,
				ResponseTime: time.Now().Unix(),
				Message:      "",
			}, nil
		} else {
			// 如果设置了密钥，HTTP传输的数据必须进行加密
			_bytes, err := mq.broker.Crypto().Decrypt(decode)
			if err != nil {
				mq.Logger().Warn(clientIp, "has correct token, but value decrypt failed: ", err)
				return &ProductResponse{
					Status:       proto.GetMessageResponseStatusText(proto.TokenIncorrectStatus),
					Offset:       0,
					ResponseTime: time.Now().Unix(),
					Message:      err.Error(),
				}, nil
			}
			pm.Value = _bytes
		}
	}

	pm.Key = []byte(form.Key)
	pm.Topic = []byte(form.Topic)

	respForm := &ProductResponse{}
	respForm.Status = proto.GetMessageResponseStatusText(proto.AcceptedStatus)
	respForm.Offset = mq.broker.Publisher(pm)
	respForm.ResponseTime = time.Now().Unix()

	mq.Logger().Debug(fmt.Sprintf("return: %s, to '%s' ", respForm, clientIp))

	return respForm, nil
}

// PostProductAsync 异步发送一个生产者消息
func (r *EdgeRouter) PostProductAsync(c *fastapi.Context, form *ProducerForm) (*ProductResponse, error) {
	return r.PostProduct(c, form)
}

// ====

type StatRouter struct {
	fastapi.BaseGroupRouter
}

func (r *StatRouter) Prefix() string { return "/api/statistic" }

func (r *StatRouter) PathSchema() pathschema.RoutePathSchema {
	return pathSchema
}

func (r *StatRouter) Summary() map[string]string {
	return map[string]string{
		"GetProducers": "获取Broker内的生产者连接",
	}
}

func (r *StatRouter) Description() map[string]string {
	return map[string]string{}
}

// GetProducers 获取Broker内的生产者连接
func (r *StatRouter) GetProducers(c *fastapi.Context) ([]string, error) {
	return mq.Stat().Producers(), nil
}

// GetConsumers 获取Broker内的消费者连接
func (r *StatRouter) GetConsumers(c *fastapi.Context) ([]*ConsumerStatistic, error) {
	cs := mq.Stat().ConsumerTopics()
	tcs := make([]*ConsumerStatistic, len(cs))
	for i := 0; i < len(cs); i++ {
		tcs[i] = &ConsumerStatistic{
			Addr:   cs[i].Addr,
			Topics: cs[i].Topics,
		}
	}

	return tcs, nil
}

// GetTopic 获取Broker内的topic名称
func (r *StatRouter) GetTopic(c *fastapi.Context) ([]string, error) {
	return mq.Stat().TopicsName(), nil
}

// GetTopicOffset 获取Broker内的topic名称及其最新的消息计数
func (r *StatRouter) GetTopicOffset(c *fastapi.Context) ([]*TopicOffsetStatistic, error) {
	ss := mq.Stat().TopicsOffset()

	form := make([]*TopicOffsetStatistic, len(ss))
	for i := 0; i < len(ss); i++ {
		form[i] = &TopicOffsetStatistic{Topic: ss[i].Name, Offset: ss[i].Offset}
	}

	return form, nil
}

// GetTopicRecord 获取主题内部的最新消息记录
func (r *StatRouter) GetTopicRecord(c *fastapi.Context) ([]*TopicRecordStatistic, error) {
	records := mq.Stat().LatestRecord()

	cs := make([]*TopicRecordStatistic, len(records))
	for i := 0; i < len(records); i++ {
		record := records[i]
		cs[i] = &TopicRecordStatistic{
			Topic:       string(record.Topic),
			Offset:      record.Offset,
			Key:         string(record.Key),
			Value:       helper.Base64Encode(record.Value),
			ProductTime: record.Time,
		}
	}

	return cs, nil
}

// GetTopicConsumers 获取主题内部的消费者连接
func (r *StatRouter) GetTopicConsumers(c *fastapi.Context) ([]*TopicConsumerStatistic, error) {
	cs := mq.Stat().TopicConsumers()
	cc := make([]*TopicConsumerStatistic, len(cs))
	for i := 0; i < len(cs); i++ {
		cc[i] = &TopicConsumerStatistic{
			Topic:     cs[i].Name,
			Consumers: cs[i].Consumers,
		}
	}

	return cc, nil
}

// ====

type OperationResult struct {
	Result string `json:"result" validate:"required,oneof=ok fail" description:"删除结果"`
	Err    string `json:"err,omitempty" description:"错误原因"`
}

func Result(err error) *OperationResult {
	if err != nil {
		return &OperationResult{
			Result: "fail",
			Err:    err.Error(),
		}
	}

	return &OperationResult{
		Result: "ok",
	}
}

type TopicDeleteReq struct {
	Topic string `json:"topic" validate:"required,gte=1" description:"主题名称"`
	Force bool   `json:"force" description:"强制删除主题，默认情况下如果topic有消费者连接，则不允许删除，设为true则首先关闭连接后再删除"`
}

type ExchangeReq struct {
	From string `json:"from" validate:"required,gte=1" description:"源TOPIC"`
	To   string `json:"to" validate:"required,gte=1" description:"目标TOPIC"`
}

func (m *ExchangeReq) String() string {
	return fmt.Sprintf("<ExchangeReq> from [ %s ] to [ %s ]", m.From, m.To)
}

func (m *ExchangeReq) SchemaDesc() string {
	return "设置topic数据交换"
}

type ExchangeResp struct {
	OperationResult
	//Code   string `json:"code,omitempty" description:"错误码"`
	//ZhErr  string `json:"zh_err,omitempty" description:"中文错误信息"`
}

type ExchangeShowReq struct {
	Via string `query:"via" json:"via" description:"源TOPIC,空则查询所有"`
}

func (m *ExchangeResp) String() string {
	return fmt.Sprintf("<ExchangeResp> result [ %s ]", m.Result)
}

func (m *ExchangeResp) SchemaDesc() string {
	return "交换结果"
}

type ExchangeShowResp struct {
	From string `json:"from" validate:"required,gte=1" description:"源TOPIC"`
	To   string `json:"to" validate:"required,gte=1" description:"目标TOPIC"`
}

type ExchangeRouter struct {
	fastapi.BaseGroupRouter
}

func (r *ExchangeRouter) Prefix() string { return "/api/exchange" }

func (r *ExchangeRouter) PathSchema() pathschema.RoutePathSchema {
	return pathSchema
}

func (r *ExchangeRouter) Summary() map[string]string {
	return map[string]string{
		"Exchange": "交换主题",
	}
}

func (r *ExchangeRouter) Description() map[string]string {
	return map[string]string{}
}

// DeleteDelTopic 删除主题
func (r *ExchangeRouter) DeleteDelTopic(c *fastapi.Context, form *TopicDeleteReq) (*OperationResult, error) {
	err := mq.broker.DeleteTopic(form.Topic, form.Force)

	return Result(err), nil
}

func (r *ExchangeRouter) PostAddRoute(c *fastapi.Context, form *ExchangeReq) (*OperationResult, error) {
	err := mq.AddExchange(form.From, form.To)
	return Result(err), nil
}

func (r *ExchangeRouter) DeleteDelRoute(c *fastapi.Context, form *ExchangeReq) (*OperationResult, error) {
	err := mq.DelExchange(form.From, form.To)
	return Result(err), nil
}

func (r *ExchangeRouter) GetShowRoute(c *fastapi.Context, form *ExchangeShowReq) ([]*ExchangeShowResp, error) {
	results := make([]*ExchangeShowResp, 0)

	if form.Via == "" {
		// 查询所有topic
		mq.broker.RangeTopic(func(topic *engine.Topic) bool {
			topic.RangeForwarding(func(to *engine.Topic) bool {
				results = append(results, &ExchangeShowResp{
					From: string(topic.Name),
					To:   string(to.Name),
				})
				return true
			})

			return true
		})

	} else {
		topic, exist := mq.broker.FindTopic(form.Via)
		if !exist {
			return nil, engine.ErrSrcNotExist
		}

		topic.RangeForwarding(func(to *engine.Topic) bool {
			results = append(results, &ExchangeShowResp{
				From: string(topic.Name),
				To:   string(to.Name),
			})
			return true
		})
	}

	return results, nil
}

// ====

func ErrorLog(c *fastapi.Context) {
	//mq.Logger().Info(fmt.Sprintf("path: '%s' elapsed time: %s",
	//	c.MuxContext().Path(), time.Now().Sub(c.GetTime(config.KeyElapsedTime))))

	if c.Response().StatusCode != http.StatusOK && c.Response().StatusCode != http.StatusUnauthorized {
		if strings.HasPrefix(c.Response().ContentType, openapi.MIMEApplicationJSON) {
			bs, _ := c.Marshal(c.Response().Content)
			mq.Logger().Warn(fmt.Sprintf("path: %s, error: %s", c.MuxContext().Path(), bs))
		}
	}

	return
}

type ErrorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func ErrorFormatter(c *fastapi.Context, err error) (int, any) {
	return 400, &ErrorResponse{
		Code:    "400",
		Message: err.Error(),
	}
}
