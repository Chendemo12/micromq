<script lang="ts" setup>
import {Notebook} from "@element-plus/icons-vue";
</script>


<template>
  <el-menu
      :ellipsis="false"
      class="el-menu-demo"
      mode="horizontal"
      router
  >
    <el-menu-item index="content">
      <div class="bar-title">
        <slot>Clipboard Reader</slot>
        <el-icon>
          <Notebook style="margin-left: 10px; width: 1em; height: 1em"></Notebook>
        </el-icon>
      </div>
    </el-menu-item>

    <div class="flex-grow"/>
  </el-menu>
</template>


<style scoped>
.bar-title {
  font-size: 20px;
  margin-left: 40px;
  margin-right: 20px;
  display: flex;
  justify-content: center; /* 水平居中 */
  align-items: center; /* 垂直居中 */
}

.flex-grow {
  flex-grow: 1;
}

</style>
