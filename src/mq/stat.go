package mq

import (
	"github.com/Chendemo12/fastapi"
	"github.com/Chendemo12/fastapi/pathschema"
	"github.com/Chendemo12/functools/helper"
)

type ConsumerStatistic struct {
	fastapi.BaseModel
	Addr   string   `json:"addr" description:"连接地址"`
	Topics []string `json:"topics" description:"订阅的主题名列表"`
}

func (m *ConsumerStatistic) SchemaDesc() string {
	return "消费者统计信息"
}

type TopicOffsetStatistic struct {
	fastapi.BaseModel
	Topic  string `json:"topic" description:"名称"`
	Offset uint64 `json:"offset" description:"最新的消息偏移量"`
}

func (m *TopicOffsetStatistic) SchemaDesc() string {
	return "Topic的消息偏移量信息"
}

type TopicRecordStatistic struct {
	fastapi.BaseModel
	Topic       string `json:"topic" description:"名称"`
	Key         string `json:"key"`
	Value       string `json:"value" description:"base64编码后的消息体明文"`
	Offset      uint64 `json:"offset" description:"消息偏移量"`
	ProductTime int64  `json:"product_time" description:"消息接收时间戳"`
}

type TopicConsumerStatistic struct {
	fastapi.BaseModel
	Topic     string   `json:"topic" description:"名称"`
	Consumers []string `json:"consumers" description:"消费者连接"`
}

type StatRouter struct {
	fastapi.BaseGroupRouter
}

func (r *StatRouter) Prefix() string {
	return "/api/statistic"
}

func (r *StatRouter) Tags() []string {
	return []string{"STATISTIC"}
}

func (r *StatRouter) PathSchema() pathschema.RoutePathSchema {
	return pathschema.LowerCaseBackslash
}

func (r *StatRouter) Summary() map[string]string {
	return map[string]string{
		"GetProducers":      "获取Broker内的生产者连接",
		"GetConsumers":      "获取Broker内的消费者连接",
		"GetTopic":          "获取Broker内的topic名称",
		"GetTopicOffset":    "获取Broker内的topic名称及其最新的消息计数",
		"GetTopicRecord":    "获取主题内部的最新消息记录",
		"GetTopicConsumers": "获取主题内部的消费者连接",
	}
}

func (r *StatRouter) Description() map[string]string {
	return map[string]string{
		"PostProduct":      "阻塞式发送生产者消息，此接口会在消息成功发送给消费者后返回",
		"PostProductAsync": "非阻塞式发送生产者消息，服务端会在消息解析成功后立刻返回结果，不保证消息已发送给消费者",
	}
}

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

func (r *StatRouter) GetTopic(c *fastapi.Context) ([]string, error) {
	return mq.Stat().TopicsName(), nil
}

func (r *StatRouter) GetTopicOffset(c *fastapi.Context) ([]*TopicOffsetStatistic, error) {
	ss := mq.Stat().TopicsOffset()

	form := make([]*TopicOffsetStatistic, len(ss))
	for i := 0; i < len(ss); i++ {
		form[i] = &TopicOffsetStatistic{Topic: ss[i].Name, Offset: ss[i].Offset}
	}

	return form, nil
}

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
