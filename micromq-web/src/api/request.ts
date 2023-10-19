import axios, { type AxiosResponse } from "axios";
import { ref } from "vue";

const ApiPrefix = import.meta.env.VITE_API_URL;

// 允许跨域
axios.defaults.baseURL = ApiPrefix;
axios.defaults.withCredentials = false;
axios.defaults.timeout = 5000;

/**
 * 封装get方法
 */
export function Get(url: string, params = {}) {
  return new Promise((resolve, reject) => {
    axios
      .get(url, { params: params })
      .then((response) => {
        resolve(response);
      })
      .catch((err) => {
        reject(err);
      });
  });
}

/**
 * 封装post方法
 */
export function Post(url: string, data = {}) {
  return new Promise((resolve, reject) => {
    axios
      .post(url, data)
      .then((response) => {
        resolve(response);
      })
      .catch((err) => {
        reject(err);
      });
  });
}

export function Fetch(url: string) {
  const data = ref(null);
  const error = ref(null);

  Get(url)
    .then((res) => res.json())
    .then((json) => (data.value = json))
    .catch((err) => (error.value = err));

  return { data, error };
}

export default {
  ApiPrefix,
  Get,
  Post,
  Fetch,
};
