<script lang="ts" setup>
import {RouterView} from "vue-router";
import BaseBar from "@/components/BaseBar.vue";
import HomeSide from "@/components/home/Aside.vue";
import Footer from "@/components/Footer.vue";
</script>

<template>
  <div class="common-layout">
    <el-container style="height: 100%">
      <el-header>
        <nav>
          <BaseBar>MICROMQ</BaseBar>
        </nav>
      </el-header>

      <el-container class="body-container">
        <el-aside width="200px">
          <HomeSide></HomeSide>
        </el-aside>
        <el-main class="body-container-main">
          <RouterView></RouterView>
        </el-main>
      </el-container>

      <Footer></Footer>
    </el-container>
  </div>
</template>


<style scoped>

.common-layout {
  height: 100%;
}

.body-container {
  height: 100%;
}

</style>