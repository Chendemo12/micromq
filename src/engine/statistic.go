package engine

type Statistic struct {
	broker *Engine
}

// TopicsName 获取当前全部Topic名称
func (k Statistic) TopicsName() []string {
	topics := make([]string, 0)

	k.broker.RangeTopic(func(topic *Topic) bool {
		topics = append(topics, string(topic.Name))
		return true
	})

	return topics
}

type TopicOffset struct {
	Name   string `json:"name" description:"名称"`
	Offset uint64 `json:"offset" description:"最新的消息偏移量"`
}

// TopicsOffset 获取全部的Topic以及响应的偏移量
func (k Statistic) TopicsOffset() []*TopicOffset {
	topics := make([]*TopicOffset, 0)
	k.broker.RangeTopic(func(topic *Topic) bool {
		topics = append(topics, &TopicOffset{
			Name:   string(topic.Name),
			Offset: topic.Offset,
		})

		return true
	})

	return topics
}

// TopicConsumer topic内的消费者
type TopicConsumer struct {
	Name      string   `json:"name" description:"名称"`
	Consumers []string `json:"consumers" description:"消费者连接"`
}

// TopicConsumers 获取topic内的消费者连接
func (k Statistic) TopicConsumers() []*TopicConsumer {
	consumers := make([]*TopicConsumer, 0)
	k.broker.RangeTopic(func(topic *Topic) bool {
		consumer := &TopicConsumer{}
		consumer.Name = string(topic.Name)
		consumer.Consumers = make([]string, 0)
		topic.RangeConsumer(func(c *Consumer) bool {
			consumer.Consumers = append(consumer.Consumers, c.Addr)
			return true
		})
		consumers = append(consumers, consumer)
		return true
	})

	return consumers
}

// ConsumerTopic 消费者订阅的topic
type ConsumerTopic struct {
	Addr   string   `json:"addr" description:"连接地址"`
	Topics []string `json:"topics" description:"订阅的主题名列表"`
}

// ConsumerTopics 获取消费者订阅的主题名
func (k Statistic) ConsumerTopics() []*ConsumerTopic {
	cts := make([]*ConsumerTopic, 0)

	k.broker.RangeConsumer(func(c *Consumer) bool {
		ct := &ConsumerTopic{
			Addr:   c.Addr,
			Topics: make([]string, len(c.Conf.Topics)),
		}
		copy(ct.Topics, c.Conf.Topics)
		cts = append(cts, ct)
		return true
	})

	return cts
}

// LatestRecord 获取topic的最新消息记录
func (k Statistic) LatestRecord() []*HistoryRecord {
	messages := make([]*HistoryRecord, 0)

	k.broker.RangeTopic(func(topic *Topic) bool {
		record := topic.LatestMessage()
		if record != nil {
			messages = append(messages, record)
		}

		return true
	})

	return messages
}

// Producers 获取全部生产者连接信息
func (k Statistic) Producers() []string {
	ps := make([]string, 0)
	k.broker.RangeProducer(func(p *Producer) bool {
		ps = append(ps, p.Addr)
		return true
	})

	return ps
}
