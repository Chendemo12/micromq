<template>
  <el-table
      :data="topicRecord"
      :default-sort="{ prop: 'product_time', order: 'descending' }"
      @cell-click="recordClick"
      highlight-current-row
      stripe
      style="width: 100%; font-size: 16px"
  >
    <el-table-column type="index" label="序号" width="100"/>
    <el-table-column prop="topic"
                     label="主题"
                     sortable
                     width="200"
    >
      <template #default="scope">
        <el-popover effect="light" trigger="hover" placement="top" width="auto">
          <template #default>
            <div>名称: {{ scope.row.topic }}</div>
            <div>消费者：{{ filterTopicConsumers(scope.row.topic).consumers }}</div>
          </template>
          <template #reference>
            {{ scope.row.topic }}
          </template>
        </el-popover>
      </template>

    </el-table-column>
    <el-table-column prop="offset" label="偏移量" width="100"/>
    <el-table-column prop="key" label="键名" width="180"/>
    <el-table-column prop="value" label="消息" width="auto">
      <template #default="scope">
        <el-popover effect="dark" trigger="hover" placement="top" width="auto">
          <template #default>
            <div>{{ base64Unmarshal(scope.row.value) }}</div>
          </template>
          <template #reference>
            {{ scope.row.value }}
          </template>
        </el-popover>
      </template>
    </el-table-column>
    <el-table-column prop="product_time"
                     sortable
                     label="消费时间"
                     :formatter="recordProductTimeFormatter"
                     width="180">

    </el-table-column>
  </el-table>
</template>

<script lang="ts" setup>

import {Get} from "@/api/request";
import {Urls} from "@/api/client";
import {onMounted, ref} from "vue";
import type {TableColumnCtx} from "element-plus";
import {ElPopover} from "element-plus";


// 绘制为折线图
const topicOffset = ref([
  {
    topic: "",
    offset: ""
  }
])

interface Record {
  topic: string
  key: string
  value: string
  offset: bigint
  product_time: bigint
}

interface Consumer {
  topic: string
  consumers: Array<string>
}

// 显示为表格
const topicRecord: ref<Array<Record>> = ref([
  {
    "topic": "",
    "key": "",
    "value": "",
    "offset": 0,
    "product_time": 1697792669
  },
])

const topicConsumer: ref<Array<Consumer>> = ref([
  {
    "topic": "",
    "consumers": []
  }
])

function filterTopicConsumers(topic: string): Consumer {
  for (let i = 0; i < topicConsumer.value.length; i++) {
    if (topicConsumer.value[i].topic === topic) {
      return {
        topic: topic,
        consumers: topicConsumer.value[i].consumers
      }
    }
  }
  return {
    "topic": "",
    "consumers": []
  }
}


function formatTime(timestamp: number): string {
  const date = new Date(timestamp * 1000); // 将秒转换为毫秒
  return date.toLocaleString()
}

function base64Unmarshal(base64String: string): string {
  // 反序列化为原始字符串
  return atob(base64String)
}

function recordProductTimeFormatter(row: Record, column: TableColumnCtx<Record>): string {
  return formatTime(row.product_time)
}

function recordClick(row: Record, column: TableColumnCtx<Record>, cell, event: PointerEvent): void {
  if (column.property === "value") {
    ElPopover({
      "title": "消息",
      "ref": "popoverRef",
      "trigger": "click",
      "title": "With title",
    })
  } else if (column.property === "topic") {
    const consumers = filterTopicConsumers(row.topic)
    console.log(consumers)
  }


  console.log(column.property)
}

function SaveFile(file: File): void {
  const a = document.createElement('a');
  a.href = URL.createObjectURL(file);
  a.download = file.name;
  a.click();
  URL.revokeObjectURL(a.href);
}

function getTopicOffset(): void {
  Get(Urls.getTopicOffset)
      .then((resp) => {
        topicOffset.value = resp
      })
      .catch((err) => {
        console.warn("get topic offset failed: ", err.code)
        console.log(err.request)
      })
}

function getTopicRecord(): void {
  Get(Urls.getTopicRecord)
      .then((resp) => {
        topicRecord.value = resp
      })
      .catch((err) => {
        console.warn("get topic record failed: ", err.code)
        console.log(err.request)
      })
}

function getTopicConsumer(): void {
  Get(Urls.getTopicConsumer)
      .then((resp) => {
        topicConsumer.value = resp
      })
      .catch((err) => {
        console.warn("get topic consumer failed: ", err.code)
        console.log(err.request)
      })
}

const refresh = () => {
  getTopicOffset()
  getTopicRecord()
}


onMounted(() => {
  setInterval(getTopicOffset, 2000)// 2s
  setInterval(getTopicConsumer, 2000)// 2s
  setInterval(getTopicRecord, 10000)// 10s
})

refresh()
</script>


<style scoped>

.text-container pre {
  height: 220px;
  padding-top: 0;
  margin-top: 0;
  white-space: pre-wrap; /* 允许文本换行 */
  overflow: hidden; /* 隐藏溢出部分的文本 */
  text-overflow: ellipsis; /* 显示省略号 */
}

</style>