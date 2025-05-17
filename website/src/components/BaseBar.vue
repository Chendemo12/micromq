<script lang="ts" setup>

import {Notebook} from "@element-plus/icons-vue";
import {ref} from "vue";
import {Urls} from "@/api/client";
import {Get} from "@/api/request";

let title = ref("micro-mq")

const getBrokerTitle = (): void => {
  Get(Urls.getTitle)
      .then((resp) => {
        title.value = resp.toUpperCase()
        document.title = resp
      })
      .catch((err) => {
        console.log("get broker title failed: ", err.code)
        console.log(err.request)
      })
}

getBrokerTitle()
</script>


<template>

  <el-menu
      :ellipsis="false"
      default-active="content"
      class="el-menu-demo"
      mode="horizontal"
      router
  >
    <el-menu-item index="/">
      <div class="bar-title">
        {{ title }}
        <el-icon>
          <Notebook style="margin-left: 10px; width: 1em; height: 1em"></Notebook>
        </el-icon>
      </div>
    </el-menu-item>

    <div class="flex-grow"/>

    <el-menu-item style="width: 50px" index="content">状态</el-menu-item>
    <el-menu-item style="width: 50px" index="settings" disabled>设置</el-menu-item>

    <div class="social-links">
      <div class="svg-container">
        <a
            href="https://github.com/Chendemo12/micromq" title="GitHub">
          <svg preserveAspectRatio="xMidYMid meet" viewBox="0 0 24 24" width="1.2em" height="1.2em"
               data-v-6c8d2bba="">
            <path fill="currentColor"
                  d="M12 2C6.475 2 2 6.475 2 12a9.994 9.994 0 0 0 6.838 9.488c.5.087.687-.213.687-.476c0-.237-.013-1.024-.013-1.862c-2.512.463-3.162-.612-3.362-1.175c-.113-.288-.6-1.175-1.025-1.413c-.35-.187-.85-.65-.013-.662c.788-.013 1.35.725 1.538 1.025c.9 1.512 2.338 1.087 2.912.825c.088-.65.35-1.087.638-1.337c-2.225-.25-4.55-1.113-4.55-4.938c0-1.088.387-1.987 1.025-2.688c-.1-.25-.45-1.275.1-2.65c0 0 .837-.262 2.75 1.026a9.28 9.28 0 0 1 2.5-.338c.85 0 1.7.112 2.5.337c1.912-1.3 2.75-1.024 2.75-1.024c.55 1.375.2 2.4.1 2.65c.637.7 1.025 1.587 1.025 2.687c0 3.838-2.337 4.688-4.562 4.938c.362.312.675.912.675 1.85c0 1.337-.013 2.412-.013 2.75c0 .262.188.574.688.474A10.016 10.016 0 0 0 22 12c0-5.525-4.475-10-10-10z"></path>
          </svg>
        </a>
      </div>
    </div>

    <div style="margin-right: 60px"></div>
  </el-menu>
</template>


<style scoped>
.bar-title {
  font-size: 20px;
  margin-right: 20px;
  display: flex;
  justify-content: center; /* 水平居中 */
  align-items: center; /* 垂直居中 */
}

.flex-grow {
  flex-grow: 1;
}

.social-links {
  text-size-adjust: 100%;
  line-height: 1.4;
  font-size: 20px;
  font-weight: 400;
  padding-left: 10px;
  padding-top: 1px;
  display: flex;
  align-items: center;
}

.svg-container {
  position: relative;
}

.svg-container::before {
  content: "";
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  pointer-events: none; /* 遮罩不会阻止鼠标事件 */
}

.svg-container svg {
  filter: grayscale(100%); /* 将 SVG 图片转为灰色 */
}

</style>
