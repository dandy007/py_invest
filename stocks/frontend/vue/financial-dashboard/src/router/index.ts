import { createRouter, createWebHistory } from 'vue-router'
import StockDashboard from '../views/StockDashboard.vue'
import OptionsDashboard from '../views/OptionsDashboard.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    {
      path: '/',
      redirect: '/stocks'
    },
    {
      path: '/stocks',
      name: 'stocks',
      component: StockDashboard
    },
    {
      path: '/options',
      name: 'options',
      component: OptionsDashboard
    }
  ]
})

export default router