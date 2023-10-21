import {reactive} from 'vue'
import type {UnwrapNestedRefs} from "@vue/reactivity";

export interface OffsetOfTopic {
    topic: string
    offset: bigint
}

export interface ConsumerOfTopic {
    topic: string
    consumers: Array<string>
}

export interface RecordOfTopic {
    topic: string
    key: string
    value: string
    offset: bigint
    product_time: bigint
}

export interface Linker {
    name: string
}

export interface Consumer {
    addr: string,
    topics: Array<string>
}


export class Message implements RecordOfTopic {
    topic: string
    key: string
    value: string
    offset: bigint
    product_time: bigint

    constructor(name: string, key: string, value: string, offset: bigint, product_time: bigint) {
        this.topic = name
        this.offset = offset
        this.key = key
        this.value = value
        this.product_time = product_time
    }
}

export class TopicOffset implements OffsetOfTopic {
    topic: string
    offset: bigint

    constructor(name: string, offset: bigint) {
        this.topic = name
        this.offset = offset
    }
}


export class TopicConsumer implements ConsumerOfTopic {
    topic: string
    consumers: Array<string>

    constructor(name: string, cons: Array<string>) {
        this.topic = name
        this.consumers = cons
    }
}

export class BrokerConsumer implements Consumer {
    addr: string
    topics: Array<string>

    constructor(name: string, t: Array<string>) {
        this.addr = name
        this.topics = t
    }
}

export class Broker {
    topics: Array<Linker>
    topicsOffset: Array<TopicOffset>
    topicConsumers: Array<TopicConsumer>
    producers: Array<Linker>
    consumers: Array<BrokerConsumer>
    topicsLatestMessages: Array<Message>

    _click: bigint

    constructor() {
        this.topics = []
        this.topicsOffset = []
        this.topicConsumers = []
        this.producers = []
        this.topicsLatestMessages = []
        this.consumers = []
        this._click = 0
    }

    formatTime(timestamp: bigint): string {
        const date = new Date(timestamp * 1000); // 将秒转换为毫秒
        return date.toLocaleString()
    }

    formatTimeDifference(timestamp: bigint, offset: number): string {
        const now = Date.now();
        const difference = now - timestamp * 1000;

        if (difference < 60000) { // 不足1分钟
            const seconds = Math.floor(difference / 1000) + offset;
            return `${seconds}s ago`;
        } else if (difference < 3600000) { // 不足1小时
            const minutes = Math.floor(difference / 60000) + offset;
            return `${minutes}m ago`;
        } else if (difference < 86400000) { // 不足1天
            const hours = Math.floor(difference / 3600000) + offset;
            return `${hours}h ago`;
        } else { // 大于等于1天
            const days = Math.floor(difference / 86400000) + offset;
            return `${days}d ago`;
        }
    }

    base64Unmarshal(base64String: string): string {
        // 反序列化为原始字符串
        return atob(base64String)
    }

    filterTopicConsumers(topic: string): TopicConsumer {
        for (let i = 0; i < this.topicConsumers.length; i++) {
            if (this.topicConsumers[i].topic == topic) {
                return this.topicConsumers[i]
            }
        }
        return new TopicConsumer()
    }


    updateOffset(offset: Array<OffsetOfTopic>): void {
        this.topicsOffset.length = 0
        for (const elem of offset) {
            this.topicsOffset.push(new TopicOffset(elem.topic, elem.offset))
        }
    }

    updateTopicConsumer(elements: Array<ConsumerOfTopic>): void {
        this.topicConsumers.length = 0
        for (const element of elements) {
            this.topicConsumers.push(new TopicConsumer(element.topic, element.consumers))
        }
    }

    updateProducer(elements: Array<string>): void {
        this.producers.length = 0
        for (const element of elements) {
            this.producers.push({name: element})
        }
    }

    updateConsumer(elements: Array<Consumer>): void {
        this.consumers.length = 0
        for (const element of elements) {
            this.consumers.push(new BrokerConsumer(element.addr, element.topics))
        }
    }

    updateTopicMessage(elements: Array<RecordOfTopic>): void {
        this.topicsLatestMessages.length = 0
        for (const element of elements) {
            this.topicsLatestMessages.push(new Message(
                element.topic, element.key, element.value, element.offset, element.product_time
            ))
        }
    }

    updateTopic(elements: Array<string>): void {
        this.topics.length = 0
        for (const element of elements) {
            this.topics.push({name: element})
        }
    }

    getOffset(t: string): bigint {
        for (const message of this.topicsLatestMessages) {
            if (message.topic == t) {
                return message.offset
            }
        }

        return 0
    }

    getTopics(): Array<Linker> {
        const o: Array<Linker> = []
        for (const message of this.topicsLatestMessages) {
            o.push({name: message.topic})
        }

        return o
    }

    getTopicMessage(t: string): Message {
        for (const message of this.topicsLatestMessages) {
            if (message.topic == t) {
                return message
            }
        }

        return new Message("", "", "", 0, 0)
    }
}

export const broker: UnwrapNestedRefs<Broker> = reactive(new Broker())