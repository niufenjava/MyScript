import { createRouter, createWebHistory } from 'vue-router'
import TagsPage from '@/views/TagsPage.vue'
import SelectorPage from '@/views/SelectorPage.vue'

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    { path: '/tags', component: TagsPage },
    { path: '/selector', component: SelectorPage },
  ]
})

export default router
