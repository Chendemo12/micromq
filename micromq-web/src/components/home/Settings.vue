<script lang="ts" setup>
import {reactive, ref} from "vue";
import {Warning} from "@element-plus/icons-vue";
import {ElNotification, ElTooltip} from "element-plus";
import client from "@/api/client";
import request from "@/api/request";
import type {AxiosResponse} from "axios";

const size = ref("default");
const labelPosition = ref("right");

const modifyErrorMessage = () => {
  ElNotification({
    title: "参数修改失败",
    message: "系统参数修改失败",
    type: "error",
    position: "top-right",
  });
};

// 表单模型
const configForm = reactive({
  dashboardPort: 7077,
  remotePort: 7077,
  remoteHostPrefix: "http://",
  remoteHost: "",
  textDump: true,
  imageDump: true,
  textDumpDir: "./resources/text",
  imageDumpDir: "./resources/image",
  debug: false,
});

const refresh = () => {
  request
      .Get(client.Urls.getConfig)
      .then((response: AxiosResponse) => {
        if (response.status === 200) {
          configForm.debug = response.data.debug;
          configForm.imageDump = response.data.imageDump;
          configForm.textDump = response.data.textDump;
          configForm.dashboardPort = Number(response.data.dashboardPort);
          configForm.remoteHostPrefix = response.data.remoteHostPrefix;
          configForm.remotePort = Number(response.data.remotePort);
          configForm.remoteHost = response.data.remoteHost;

          if (response.data.imageDumpDir !== "") {
            configForm.imageDumpDir = response.data.imageDumpDir;
          }
          if (response.data.textDumpDir !== "") {
            configForm.textDumpDir = response.data.textDumpDir;
          }
        } else {
          ElNotification({
            title: "Error",
            message: "系统参数请求失败",
            type: "error",
            position: "top-right",
          });
        }
      })
      .catch((error) => {
        ElNotification({
          title: "Error",
          message: "系统参数请求失败: " + error.toString(),
          type: "error",
          position: "top-right",
        });
      });
};

const onSubmit = () => {
  request
      .Post(client.Urls.modifyConfig, configForm)
      .then(
          (response: AxiosResponse) => {
            if (response.status == 200) {
              // 更新本机缓存
              refresh();
              ElNotification({
                title: "Success",
                message: "系统参数修改成功!",
                type: "success",
                position: "top-right",
              });
            } else {
              modifyErrorMessage();
            }
          },
          (reactive) => {
            if (reactive.response.status == 422) {
              let msg = "";
              for (let i = 0; i < reactive.response.data.detail.length; i++) {
                msg += reactive.response.data.detail[i].msg + "\n";
              }
              ElNotification({
                title: "表单验证失败",
                message: msg,
                type: "error",
                position: "top-right",
              });
            } else if (reactive.status == 500) {
              ElNotification({
                title: "参数修改失败",
                message: reactive.response.data,
                type: "error",
                position: "top-right",
              });
            } else {
              modifyErrorMessage();
            }
          }
      )
      .catch((error) => {
        ElNotification({
          title: "Error",
          message: "系统参数修改失败: " + error.toString(),
          type: "error",
          position: "top-right",
        });
      });
};

// 表单验证规则
const validateRules = reactive({
  dashboardPort: [
    {
      required: false,
      message: "输入后台服务的工作端口",
      trigger: "blur",
    },
  ],
  remoteHost: [
    {
      required: true,
      message: "输入对端服务IP地址或域名",
      trigger: "change",
      pattern:
          /^((25[0-5]|2[0-4]\d|[01]?\d?\d)(\.|$)){4}|([a-zA-Z0-9]+(-[a-zA-Z0-9]+)*\.)+[a-zA-Z]{2,}$/,
    },
  ],
  remotePort: [
    {
      required: true,
      message: "输入对端服务端口",
      trigger: "blur",
    },
  ],
});

// 进入页面即请求一次数据
refresh();
</script>

<template>
  <br/>
  <el-form
      ref="form"
      :model="configForm"
      label-width="auto"
      :label-position="labelPosition"
      :size="size"
      :rules="validateRules"
      scroll-to-error
  >
    <el-form-item
        label="后台监听端口"
        prop="dashboardPort"
        style="width: 400px"
    >
      <el-input-number
          v-model="configForm.dashboardPort"
          :min="1024"
          :max="65535"
          controls-position="right"
          :size="size"
          placeholder="修改后台服务的监听端口"
          style="width: 300px"
      />
    </el-form-item>

    <el-form-item label="对端服务地址" prop="remoteHost" style="width: 400px">
      <el-input
          v-model="configForm.remoteHost"
          placeholder="输入对端服务的工作地址"
      >
        <template #prepend>
          <el-select
              v-model="configForm.remoteHostPrefix"
              placeholder="Select"
              style="width: 100px"
          >
            <el-option label="http://" value="http://"/>
            <el-option label="https://" value="https://"/>
          </el-select>
        </template>
      </el-input>
    </el-form-item>

    <el-form-item label="对端服务端口" prop="remotePort" style="width: 400px">
      <el-input-number
          v-model="configForm.remotePort"
          :min="1024"
          :max="65535"
          controls-position="right"
          :size="size"
          placeholder="输入对端服务的工作端口"
          style="width: 300px"
      />
    </el-form-item>

    <el-form-item label="文本缓存目录" style="width: 400px">
      <el-input v-model="configForm.textDumpDir" webkitdirectory></el-input>
    </el-form-item>
    <el-form-item label="图片缓存目录" style="width: 400px">
      <el-input v-model="configForm.imageDumpDir" webkitdirectory></el-input>
    </el-form-item>

    <el-form-item label="开启文本历史记录">
      <el-switch
          v-model="configForm.textDump"
          class="ml-2"
          style="--el-switch-on-color: #13ce66; --el-switch-off-color: #ff4949"
      />
      <el-tooltip
          content="仅当此开关开启时，才能在历史记录-文本中查看到历史记录"
          effect="dark"
          placement="top"
      >
        <el-icon :size="12" style="margin-left: 4px">
          <Warning/>
        </el-icon>
      </el-tooltip>
    </el-form-item>

    <el-form-item label="开启截图历史记录">
      <el-switch
          v-model="configForm.imageDump"
          class="ml-2"
          style="--el-switch-on-color: #13ce66; --el-switch-off-color: #ff4949"
      />
      <el-tooltip
          content="仅当此开关开启时，才能在历史记录-图片中查看到历史记录"
          effect="dark"
          placement="top"
      >
        <el-icon :size="12" style="margin-left: 4px">
          <Warning/>
        </el-icon>
      </el-tooltip>
    </el-form-item>

    <el-form-item label="调试开关">
      <el-switch
          v-model="configForm.debug"
          class="ml-2"
          style="--el-switch-on-color: #13ce66; --el-switch-off-color: grey"
      />
    </el-form-item>

    <!--    -->
    <el-form-item>
      <el-button type="primary" @click="refresh">刷新</el-button>
      <el-button type="primary" @click="onSubmit">保存修改</el-button>
    </el-form-item>
  </el-form>
</template>

<style scoped></style>
