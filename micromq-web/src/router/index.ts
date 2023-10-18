import {createRouter, createWebHistory} from 'vue-router'
import HomeView from '../views/HomeView.vue'

const router = createRouter({
    history: createWebHistory(import.meta.env.BASE_URL),
    routes: [
        {
            path: "/",
            redirect: "/content",
        },
        {
            path: "/home",
            redirect: "/",
        },
        {
            path: '/',
            name: 'home',
            component: HomeView,
            meta: {
                title: "首页",
            },
            children: [
                {
                    name: "Content",
                    path: "content",
                    props: true,
                    component: () => import("@/components/home/Content.vue"),
                    meta: {
                        title: "内容",
                    },
                },
                {
                    name: "TextHistory",
                    path: "history-text",
                    props: true,
                    component: () => import("@/components/home/TextHistory.vue"),
                    meta: {
                        title: "文本记录",
                    },
                },
                {
                    name: "ImageHistory",
                    path: "history-image",
                    props: true,
                    component: () => import("@/components/home/ImageHistory.vue"),
                    meta: {
                        title: "图片记录",
                    },
                },
                {
                    name: "Settings",
                    path: "settings",
                    props: true,
                    component: () => import("@/components/home/Settings.vue"),
                    meta: {
                        title: "设置",
                    },
                }
            ]
        },
    ]
})

export default router
