<script lang="ts" setup>
import {onMounted, ref} from "vue";
import {saveAs} from "file-saver"
import {ElBadge, ElDrawer, ElImage, ElNotification} from "element-plus";
import {Bottom, Refresh, View, Warning} from "@element-plus/icons-vue";
import request from "@/api/request";
import {AxiosResponse} from "axios";
import client from "@/api/client";


// 详细内容抽屉的打开方向: 右侧
const direction = ref('rtl')
const showTextDetailWindow = ref(false)

let CurrentClipboard = {
  "kind": ref("text"),
  "text": ref(""),
  "timestamp": ref(1696413603),
  "from": ref("localhost"),
  "result": ref(true),
  "message": ref(""),
  "imageBase64": ref("data:image/png;base64,")
}

// 是否显示“显示”按钮
const showTextDetailButton = (): boolean => CurrentClipboard.text.value == ""


const formatTime = (timestamp: number): string => {
  const date = new Date(timestamp * 1000); // 将秒转换为毫秒
  // 转换为本地时间字符串
  return date.toLocaleString()
}

function SaveText(text: string) {
  let content = new Blob([text], {type: 'text/plain;charset=utf-8'});
  const timestamp = Date.now();
  const filename = new Date(timestamp).toISOString().replace(/[-:]/g, '');

  saveAs(content, filename + ".txt");

  ElNotification({
    title: 'Success',
    message: filename + ".txt" + " 保存成功!",
    type: 'success',
    position: 'top-right',
  })
}

function SaveFile(file: File): void {
  const a = document.createElement('a');
  a.href = URL.createObjectURL(file);
  a.download = file.name;
  a.click();
  URL.revokeObjectURL(a.href);
}

function SaveBase64Image(base64Data: string, filename: string): void {
  const canvas = document.createElement('canvas');
  const ctx = canvas.getContext('2d');
  const img = new Image();
  img.src = base64Data;
  img.onload = function () {
    canvas.width = img.width;
    canvas.height = img.height;
    ctx.drawImage(img, 0, 0);
    canvas.toBlob(function (blob) {
      // 将Blob对象转换为文件对象
      const file = new File([blob], filename + ".jpg", {type: 'image/jpeg'});
      // 保存文件
      SaveFile(file);
    }, 'image/jpeg');
  }
}

function SaveImage(base64Data: string): void {
  if (base64Data === "data:image/jpeg;base64,") {
    ElNotification({
      title: 'Error',
      message: "当前剪切板不存在图片",
      type: 'error',
      position: 'top-right',
    })
  } else {
    const timestamp = Date.now();
    const filename = new Date(timestamp).toISOString().replace(/[-:]/g, '');

    SaveBase64Image(base64Data, filename);

    ElNotification({
      title: 'Success',
      message: filename + ".png 保存成功!",
      type: 'success',
      position: 'top-right',
    })
  }
}

async function fetchClipboard(): boolean {
  await request.Get(client.Urls.getCurrentClipboard).then((response: AxiosResponse<any>) => {
    if (response.status === 200 && response.data.result) {
      CurrentClipboard.kind.value = response.data.kind
      CurrentClipboard.result.value = response.data.result
      CurrentClipboard.timestamp.value = response.data.timestamp
      CurrentClipboard.from.value = response.data.from
      CurrentClipboard.message.value = response.data.message

      if (response.data.kind === "text") {
        CurrentClipboard.text.value = response.data.text
      } else if (response.data.kind === "image") {
        CurrentClipboard.imageBase64.value = 'data:image/jpeg;base64,' + response.data.text
      }

      return true
    }
  }).catch((error) => {
    console.log(error)
    return false
  })
}


async function refreshClipboard() {
  const result = fetchClipboard()
  if (result) {
    ElNotification({
      title: 'Success',
      message: "刷新成功!",
      type: 'success',
      position: 'top-right',
    })
  } else {
    ElNotification({
      title: 'Error',
      message: "刷新失败",
      type: 'error',
      position: 'top-right',
    })
  }
}

const showTextBadge = (): boolean => {
  return CurrentClipboard.kind.value === "text"
}

// onMounted(() => setInterval(fetchClipboard, 10000))

fetchClipboard()// 进入页面即请求一次数据
</script>

<template>
  <div class="content" style="width: 100%">
    <!-- 显示时间来源统计信息 -->
    <div class="content-statistic">
      <!--    -->
      <div class="statistic-item" style="width: 25%;">
        <div class="title">
          <span>更新时间</span>
        </div>
        <div class="text"> {{ formatTime(CurrentClipboard.timestamp.value) }}</div>
      </div>

      <!--    -->
      <div class="statistic-item" style="width: 20%;">
        <div class="title">
          <span>数据类型</span>
          <el-tooltip
              content="显示当前剪切板的数据类型:文本或者图片"
              effect="dark"
              placement="top"
          >
            <el-icon :size="12" style="margin-left: 4px">
              <Warning/>
            </el-icon>
          </el-tooltip>
        </div>

        <div class="text"> {{ CurrentClipboard.kind.value.toLocaleUpperCase() }}</div>
      </div>

      <!--    -->
      <div class="statistic-item" style="width: 30%;">
        <div class="title">
          <span>数据来源</span>
          <el-tooltip
              content="显示当前剪切板的数据来源:本机或远程主机"
              effect="dark"
              placement="top"
          >
            <el-icon :size="12" style="margin-left: 4px">
              <Warning/>
            </el-icon>
          </el-tooltip>
        </div>

        <div class="text"> {{ CurrentClipboard.from.value.toUpperCase() }}</div>
      </div>
    </div>

    <el-space :size="16" alignment="normal" direction="horizontal" wrap>
      <!-- 最新的文本数据 -->
      <el-card class="box-card" shadow="hover" style="width: 50vh; height: 350px;">
        <template #header>
          <div class="card-header">
            <el-badge :hidden="!showTextBadge()" class="item" value="latest">
              <span>文本内容</span>
            </el-badge>

            <div class="card-header-button">
              <!-- 显示完整的数据-->
              <el-button :disabled="showTextDetailButton()" style="margin-left: 16px"
                         text
                         type="primary"
                         @click="showTextDetailWindow = true">
                <el-icon>
                  <View/>
                </el-icon>
              </el-button>
              <!-- 保存到文件-->
              <el-button :disabled="showTextDetailButton()" class="button"
                         text
                         @click="SaveText(CurrentClipboard.text.value)">
                <el-icon>
                  <Bottom/>
                </el-icon>
              </el-button>
              <!-- 刷新数据-->
              <el-button class="button"
                         text
                         @click="refreshClipboard()">
                <el-icon>
                  <Refresh/>
                </el-icon>
              </el-button>
            </div>
          </div>
        </template>

        <!-- 在文本超过视图或最大宽度设置时展示省略符-->
        <div class="text-container">
          <pre>{{ CurrentClipboard.text.value }}</pre>
        </div>

        <!--详细内容抽屉-->
        <el-drawer v-model="showTextDetailWindow" :direction="direction" title="文本数据">
          <pre style="margin-top: 0">{{ CurrentClipboard.text.value }}</pre>
          <template #footer>
            <div style="flex: auto">
              <el-button type="primary" @click="SaveText(CurrentClipboard.text.value)">保存</el-button>
            </div>
          </template>
        </el-drawer>
      </el-card>

      <!-- 最新的图片数据 -->
      <el-card class="box-card" shadow="hover" style="width: 50vh; height: 350px">
        <template #header>
          <div class="card-header">
            <el-badge :hidden="showTextBadge()" class="item" value="latest">
              <span>图片内容</span>
            </el-badge>

            <div class="card-header-button">
              <!-- 保存到文件-->
              <el-button :disabled="showTextBadge()" class="button"
                         text
                         @click="SaveImage(CurrentClipboard.imageBase64.value)">
                <el-icon>
                  <Bottom/>
                </el-icon>
              </el-button>
            </div>
          </div>
        </template>

        <div class="image-container">
          <el-image
              :initial-index="0"
              :preview-src-list="[CurrentClipboard.imageBase64.value]"
              :src="CurrentClipboard.imageBase64.value"
              :zoom-rate="1.2"
              fit="fill"
              hide-on-click-modal
          />
        </div>
      </el-card>
    </el-space>
  </div>
</template>

<style scoped>

.content-statistic {
  float: left;
  width: 100%;
  min-width: 600px;
  margin-bottom: 50px;
  font-family: SansSerif, "DejaVu Math TeX Gyre", serif;
}

.statistic-item {
  float: left;
  margin: 10px;
  height: 100px;
  border-radius: 4px;
}

.statistic-item .title {
  text-align: left;
  font-weight: 400;
  font-size: 12px;
  line-height: 20px;
  color: #606266;
}

.statistic-item .text {
  font-weight: 400;
  font-size: 28px;
  margin-top: 20px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.text-container {
  height: 220px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.image-container {
  height: 220px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.text-container pre {
  height: 220px;
  padding-top: 0;
  margin-top: 0;
  white-space: pre-wrap; /* 允许文本换行 */
  overflow: hidden; /* 隐藏溢出部分的文本 */
  text-overflow: ellipsis; /* 显示省略号 */
}

</style>