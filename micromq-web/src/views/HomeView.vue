<script lang="ts" setup>
import {RouterView} from "vue-router";
import HomeSide from "@/components/home/Aside.vue";
</script>

<template>
  <el-main class="body-container-main">
    <el-container>
      <el-aside width="200px">
        <HomeSide/>
      </el-aside>
      <el-main>
        <RouterView></RouterView>
      </el-main>
    </el-container>
  </el-main>
</template>


<style scoped>


</style>