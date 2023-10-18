<script lang="ts" setup>
import {FolderOpened, List, Menu as IconMenu, Setting,} from '@element-plus/icons-vue'
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
            <icon-menu/>
          </el-icon>
          <span>当前内容</span>
        </el-menu-item>

        <el-sub-menu index="">
          <template #title>
            <el-icon>
              <FolderOpened/>
            </el-icon>
            <span>历史记录</span>
          </template>
          <el-menu-item index="history-text">
            <el-icon>
              <List/>
            </el-icon>
            <span>文本</span>
          </el-menu-item>
          <el-menu-item index="history-image">
            <el-icon>
              <Picture/>
            </el-icon>
            <span>图片</span>
          </el-menu-item>
        </el-sub-menu>

        <el-menu-item index="settings">
          <el-icon>
            <setting/>
          </el-icon>
          <span>配置修改</span>
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
