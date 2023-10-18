<script lang="ts" setup>

const GetDashboardAddr = (): Array<string> => {
  return [import.meta.env.VITE_API_URL, import.meta.env.VITE_API_URL + "/docs"]
}

const OpenDashboardDoc = () => {
  const link = import.meta.env.VITE_API_URL + "/docs"
  window.open(link)
}

</script>

<template>
  <el-footer class="text-muted" height="40px">
    <p> 后台地址:
      <span @click="OpenDashboardDoc()" style="cursor: pointer; color: #323333; ">
      {{ GetDashboardAddr()[0] }}
      </span>
    </p>
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

.text-muted p {
  margin-top: 10px;
  text-align: left;
}

.text-muted p a {
  font-size: 15px;
  color: grey;
  float: left;
  margin-left: 20px;
  text-decoration: none;
}

</style>
