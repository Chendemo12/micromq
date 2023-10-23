<script lang="ts" setup>
import {RouterView} from "vue-router";
import BaseBar from "@/components/BaseBar.vue";
import Footer from "@/components/Footer.vue";
</script>


<template>
  <el-container style="height: 100%; width: 100%">
    <el-header>
      <BaseBar></BaseBar>
    </el-header>

    <el-container class="body-container">
      <RouterView></RouterView>
    </el-container>

    <Footer></Footer>
  </el-container>
</template>


<style scoped>

.body-container {
  height: 100%;
}

</style>