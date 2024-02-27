<script lang="ts" setup>

import {ref} from "vue";
import {Fetch} from "@/api/request";
import {Urls} from "@/api/client";

let version = ref("v1.0.0")

const GetDashboardAddr = (): Array<string> => {
  return [import.meta.env.VITE_API_URL, import.meta.env.VITE_API_URL + "/docs"]
}

const OpenDashboardDoc = () => {
  const link = import.meta.env.VITE_API_URL + "/docs"
  window.open(link)
}


const getBrokerVersion = (): void => {
  const {data, error} = Fetch(Urls.getVersion)
  if (error.value == null) {
    version = data
  }
}


getBrokerVersion()

</script>

<template>
  <el-footer class="text-muted" height="40px">
    <div class="footer-left"> 后台地址:
      <span @click="OpenDashboardDoc()" style="cursor: pointer; color: #323333; ">
      {{ GetDashboardAddr()[0] }}
      </span>
    </div>

    <div class="version">
      <span>{{ version }}</span>
    </div>
  </el-footer>
</template>

<style>

.text-muted {
  width: 100%;
  padding-left: 100px;
  position: fixed;
  bottom: 0;
  background-color: #e8e7e7;
}

.text-muted .footer-left {
  margin-top: 10px;
  text-align: left;
  float: left;
  color: #5d5d60;
}

.version {
  margin-top: 10px;
  float: right;
  font-size: 15px;
  color: #5d5d60;
  margin-right: 20px;
  justify-content: center;
  align-items: center;
}
</style>
