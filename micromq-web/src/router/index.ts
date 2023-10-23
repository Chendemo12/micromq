import {createRouter, createWebHistory} from 'vue-router'
import HomeView from '../views/HomeView.vue'
import SettingsView from "@/views/SettingsView.vue";

const router = createRouter({
    history: createWebHistory(import.meta.env.BASE_URL),
    routes: [
        {
            path: "/home",
            redirect: "/",
        },
        {
            path: "/",
            redirect: "/content"
        },
        // routes
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
                    component: () => import("@/components/home/Topic.vue"),
                    meta: {
                        title: "主题",
                    },
                },
                {
                    name: "Consumer",
                    path: "consumer",
                    props: true,
                    component: () => import("@/components/home/Consumer.vue"),
                    meta: {
                        title: "消费者",
                    },
                },
            ]
        },
        {
            path: '/settings',
            name: 'settings',
            component: SettingsView,
            meta: {
                title: "设置",
            },
            children: []
        },
    ]
})

export default router
