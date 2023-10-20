<script lang="ts" setup>
import {Histogram, UserFilled} from '@element-plus/icons-vue'
</script>

<template>
  <el-row class="tac">
    <el-col :span="18">
      <el-menu
          class="el-menu-vertical-demo"
          default-active="content"
          router
      >
        <el-menu-item index="content">
          <el-icon>
            <Histogram/>
          </el-icon>
          <span>主题</span>
        </el-menu-item>

        <el-menu-item index="consumer">
          <el-icon>
            <UserFilled/>
          </el-icon>
          <span>消费者</span>
        </el-menu-item>
      </el-menu>
    </el-col>

  </el-row>
</template>

<style scoped>
.tac {
  height: 100%;
  padding-top: 100px;
}
</style>
