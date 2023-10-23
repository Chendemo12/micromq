package mq

import (
	"github.com/Chendemo12/fastapi"
	"github.com/Chendemo12/fastapi-tool/helper"
)

var List = fastapi.List

func StatRouter() *fastapi.Router {
	router := fastapi.APIRouter("/api/statistic", []string{"Statistic"})
	{
		router.Get("/producers", getProducers, opt{
			Summary:       "获取Broker内的生产者连接",
			ResponseModel: fastapi.Strings,
		})

		router.Get("/consumers", getConsumers, opt{
			Summary:       "获取Broker内的消费者连接",
			ResponseModel: List(&ConsumerStatistic{}),
		})

		router.Get("/topic", getTopicsName, opt{
			Summary:       "获取Broker内的topic名称",
			ResponseModel: fastapi.Strings,
		})

		router.Get("/topic/offset", getTopicsOffset, opt{
			Summary:       "获取Broker内的topic名称及其最新的消息计数",
			ResponseModel: List(&TopicOffsetStatistic{}),
		})

		router.Get("/topic/record", getTopicsMessage, opt{
			Summary:       "获取主题内部的最新消息记录",
			ResponseModel: List(&TopicRecordStatistic{}),
		})
		router.Get("/topic/consumers", getTopicConsumer, opt{
			Summary:       "获取主题内部的消费者连接",
			ResponseModel: List(&TopicConsumerStatistic{}),
		})
	}
	return router
}

func getTopicsName(c *fastapi.Context) *fastapi.Response {
	return c.OKResponse(mq.Stat().TopicsName())
}

func getProducers(c *fastapi.Context) *fastapi.Response {
	return c.OKResponse(mq.Stat().Producers())
}

type ConsumerStatistic struct {
	fastapi.BaseModel
	Addr   string   `json:"addr" description:"连接地址"`
	Topics []string `json:"topics" description:"订阅的主题名列表"`
}

func (m *ConsumerStatistic) SchemaDesc() string {
	return "消费者统计信息"
}

func getConsumers(c *fastapi.Context) *fastapi.Response {
	cs := mq.Stat().ConsumerTopics()
	tcs := make([]*ConsumerStatistic, len(cs))
	for i := 0; i < len(cs); i++ {
		tcs[i] = &ConsumerStatistic{
			Addr:   cs[i].Addr,
			Topics: cs[i].Topics,
		}
	}

	return c.OKResponse(tcs)
}

type TopicOffsetStatistic struct {
	fastapi.BaseModel
	Topic  string `json:"topic" description:"名称"`
	Offset uint64 `json:"offset" description:"最新的消息偏移量"`
}

func (m *TopicOffsetStatistic) SchemaDesc() string {
	return "Topic的消息偏移量信息"
}

func getTopicsOffset(c *fastapi.Context) *fastapi.Response {
	ss := mq.Stat().TopicsOffset()

	form := make([]*TopicOffsetStatistic, len(ss))
	for i := 0; i < len(ss); i++ {
		form[i] = &TopicOffsetStatistic{Topic: ss[i].Name, Offset: ss[i].Offset}
	}

	return c.OKResponse(form)
}

type TopicRecordStatistic struct {
	fastapi.BaseModel
	Topic       string `json:"topic" description:"名称"`
	Key         string `json:"key"`
	Value       string `json:"value" description:"base64编码后的消息体明文"`
	Offset      uint64 `json:"offset" description:"消息偏移量"`
	ProductTime int64  `json:"product_time" description:"消息接收时间戳"`
}

func getTopicsMessage(c *fastapi.Context) *fastapi.Response {
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

	return c.OKResponse(cs)
}

type TopicConsumerStatistic struct {
	fastapi.BaseModel
	Topic     string   `json:"topic" description:"名称"`
	Consumers []string `json:"consumers" description:"消费者连接"`
}

func getTopicConsumer(c *fastapi.Context) *fastapi.Response {
	cs := mq.Stat().TopicConsumers()
	cc := make([]*TopicConsumerStatistic, len(cs))
	for i := 0; i < len(cs); i++ {
		cc[i] = &TopicConsumerStatistic{
			Topic:     cs[i].Name,
			Consumers: cs[i].Consumers,
		}
	}
	return c.OKResponse(cc)
}
