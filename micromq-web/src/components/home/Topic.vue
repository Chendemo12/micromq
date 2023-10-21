<script lang="ts" setup xmlns="http://www.w3.org/1999/html">

import {Get} from "@/api/request";
import {Urls} from "@/api/client";
import {onMounted, onUnmounted} from "vue";
import type {TableColumnCtx} from "element-plus";
import {ElPopover} from "element-plus";
import {broker, Message} from "@/store/broker";


function recordProductTimeFormatter(row: Message, column: TableColumnCtx<Message>): string {
  return broker.formatTime(row.product_time)
}

function recordClick(row: Message, column: TableColumnCtx<Message>, cell, event: PointerEvent): void {
  if (column.property === "value") {
  } else if (column.property === "topic") {
  }
}


function getTopicRecord(): void {
  Get(Urls.getTopicRecord)
      .then((data) => {
        broker.updateTopicMessage(data)
      })
      .catch((err) => {
        console.warn("get topic record failed: ", err.code)
        console.log(err.request)
      })
}

function getTopicConsumer(): void {
  Get(Urls.getTopicConsumer)
      .then((data) => {
        broker.updateTopicConsumer(data)
      })
      .catch((err) => {
        console.warn("get topic consumers failed: ", err.code)
        console.log(err.request)
      })
}

const refresh = () => {
  getTopicConsumer()
  getTopicRecord()
}


onMounted(() => {
  broker._click = 0;
  setInterval(() => (broker._click += 1), 1000)
  setInterval(getTopicConsumer, 2000)// 2s
  setInterval(getTopicRecord, 10000)// 10s
})

onUnmounted(() => {
  broker._click = 0;
})

refresh()
</script>

<template>
  <el-table
      :data="broker.topicsLatestMessages"
      :default-sort="{ prop: 'product_time', order: 'descending' }"
      @cell-click="recordClick"
      highlight-current-row
      stripe
      style="width: 100%; font-size: 17px"
  >
    <el-table-column type="index" label="序号" width="50"/>
    <el-table-column prop="topic"
                     label="主题"
                     sortable
                     width="auto"
    >
      <template #default="scope">
        <el-popover trigger="hover" placement="top" width="auto">
          <template #default>
            <div class="topic-popover-title">名称:
              <span style="color: cornflowerblue">{{ scope.row.topic }}</span>
            </div>
            <div class="topic-popover-title">消费者：
              <span style="color: cornflowerblue">
                {{ broker.filterTopicConsumers(scope.row.topic).consumers }}
              </span>
            </div>
          </template>
          <template #reference>
            {{ scope.row.topic }}
          </template>
        </el-popover>
      </template>

    </el-table-column>
    <el-table-column prop="offset" label="偏移量" width="100"/>
    <el-table-column prop="key" label="键名" width="auto"/>
    <el-table-column prop="value" label="消息" width="auto">
      <template #default="scope">
        <el-popover effect="dark" trigger="hover" placement="top" width="auto">
          <template #default>
            <div style="font-size: 17px">{{ broker.base64Unmarshal(scope.row.value) }}</div>
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
                     width="auto">
    </el-table-column>

    <el-table-column prop="product_time"
                     label="标签"
                     width="100px">
      <template #default="scope">
        <el-tag
            class="mx-1"
            effect="dark"
            round
        >{{ broker.formatTimeDifference(scope.row.product_time, broker._click - broker._click) }}
        </el-tag>

      </template>

    </el-table-column>

  </el-table>
</template>


<style scoped>

.text-container pre {
  height: 220px;
  padding-top: 0;
  margin-top: 0;
  white-space: pre-wrap; /* 允许文本换行 */
  overflow: hidden; /* 隐藏溢出部分的文本 */
  text-overflow: ellipsis; /* 显示省略号 */
}

.topic-popover-title {
  color: #323333;
  font-size: 18px
}

</style>