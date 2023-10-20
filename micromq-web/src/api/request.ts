import axios, {HttpStatusCode} from "axios";
import {ref} from "vue";

const ApiPrefix = import.meta.env.VITE_API_URL;

// 允许跨域
axios.defaults.baseURL = ApiPrefix;
axios.defaults.withCredentials = false;
axios.defaults.timeout = 5000;
axios.defaults.headers.common['Access-Control-Allow-Origin'] = '*';
axios.defaults.headers.common['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE';
axios.defaults.headers.common['Access-Control-Allow-Headers'] = 'Origin, X-Requested-With, Content-Type, Accept';

/**
 * 封装get方法
 */
export function Get(url: string, params = {}): Promise<any> {
    return new Promise((resolve, reject) => {
        axios
            .get(url, {params: params})
            .then((response) => {
                    if (response.status == HttpStatusCode.Ok) {
                        resolve(response.data);
                    }
                }
            )
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

export function Fetch(url: string, params = {}) {
    const data = ref(null);
    const error = ref(null);

    axios
        .get(url, {params: params})
        .then(
            (resp) => (data.value = resp.data),
            (reason) => (error.value = reason)
        )
        .catch((err) => (error.value = err));

    return {data, error};
}
