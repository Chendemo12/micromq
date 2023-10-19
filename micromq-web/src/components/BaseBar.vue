<script lang="ts" setup>

import { Notebook } from "@element-plus/icons-vue";
import { ref } from "vue";
import client from "@/api/client";
import request from "@/api/request";
import type { AxiosResponse } from "axios";

let BrokerVersion = ref("v1.0.0")

const getVersion = () => {
  request
    .Get(client.Urls.getVersion)
    .then((response: AxiosResponse) => {
      if (response.status === 200) {
        console.log(response.data)
        BrokerVersion.value = response.data;
      }
    });
}


getVersion()

</script>


<template>
  <el-menu :ellipsis="false" class="el-menu-demo" mode="horizontal" router>
    <el-menu-item index="content">
      <div class="bar-title">
        <slot>Clipboard Reader</slot>
        <el-icon>
          <Notebook style="margin-left: 10px; width: 1em; height: 1em"></Notebook>
        </el-icon>
      </div>
    </el-menu-item>

    <div class="flex-grow" />
    <div class="version">
      <span>版本：{{ BrokerVersion }}</span>
    </div>
  </el-menu>
</template>


<style scoped>
.bar-title {
  font-size: 20px;
  margin-right: 20px;
  display: flex;
  justify-content: center;
  /* 水平居中 */
  align-items: center;
  /* 垂直居中 */
}

.flex-grow {
  flex-grow: 1;
}

.version {
  font-size: 15px;
  margin-right: 20px;
  display: flex;
  color: gray;
  justify-content: center;
  /* 水平居中 */
  align-items: center;
  /* 垂直居中 */
}
</style>
