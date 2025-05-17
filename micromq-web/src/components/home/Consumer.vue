<script setup lang="ts">

import {Get} from "@/api/request";
import {Urls} from "@/api/client";
import {broker} from "@/store/broker";
import {onMounted} from "vue";

function getBrokerProducer(): void {
  Get(Urls.getBrokerProducer)
      .then((data) => {
        broker.updateProducer(data)
      })
      .catch((err) => {
        console.warn("get broker producers failed: ", err.code)
        console.log(err.request)
      })
}


function getBrokerConsumer(): void {
  Get(Urls.getBrokerConsumer)
      .then((data) => {
        broker.updateConsumer(data)
      })
      .catch((err) => {
        console.warn("get broker consumers failed: ", err.code)
        console.log(err.request)
      })
}

const refresh = () => {
  getBrokerConsumer();
  getBrokerProducer();
}

onMounted(() => {
  setInterval(getBrokerConsumer, 10000)
  setInterval(getBrokerProducer, 10000)
})

refresh()
</script>

<template>
  <div class="link-table">
    <div class="table-title">
      <span>消费者连接</span>
    </div>

    <el-table
        :data="broker.consumers"
        highlight-current-row
        stripe
        style="width: 100%; font-size: 20px"
    >
      <el-table-column type="index" label="序号" width="100px"/>

      <el-table-column
          prop="addr"
          label="消费者"
          sortable
          width="250px"
      >
      </el-table-column>

      <el-table-column prop="topics"
                       label="订阅的主题"
                       width="auto">

        <template #default="scope">
          <el-popover v-for="item in scope.row.topics"
                      trigger="hover" placement="top" width="auto">
            <template #default>
              <div class="topic-popover-title">主题名：
                <span style="color: darkslategrey">{{ item }}</span>
              </div>

              <div class="topic-popover-title">消息偏移量:
                <span style="color: red">{{ broker.getOffset(item) }}</span>
              </div>

              <div class="topic-popover-title">最后更新时间:
                <span style="color: darkslategrey">
                  {{ broker.formatTime(broker.getTopicMessage(item).product_time) }}
                </span>
              </div>


              <div class="topic-popover-title">消息:
                <span style="color: darkslategrey">
                  {{ broker.base64Unmarshal(broker.getTopicMessage(item).value) }}
                </span>
              </div>

            </template>

            <template #reference>
              <el-tag style="margin-right: 10px" effect="light" size="default"> {{ item }}</el-tag>
            </template>
          </el-popover>
        </template>

      </el-table-column>
    </el-table>

  </div>

  <div class="link-table">
    <div class="table-title">
      <span>生产者连接</span>
    </div>

    <el-table
        :data="broker.producers"
        highlight-current-row
        stripe
        style="width: 100%; font-size: 20px"
    >
      <el-table-column type="index" label="序号" width="100px"/>

      <el-table-column
          prop="name"
          label="消费者"
          sortable
          width="auto"
      >
      </el-table-column>
    </el-table>
  </div>
</template>

<style scoped>

.link-table {
  margin-bottom: 60px;
  padding: 10px;
}

.table-title {
  font-size: 22px;
  margin-bottom: 30px;
  color: #5d5d60;
}

.topic-popover-title {
  color: #323333;
  font-size: 18px
}
</style>