<template>
  <div id="app">
    <nav class="navbar">
      <div class="nav-brand">📊 股票标签系统</div>
      <router-link to="/tags" class="nav-link">标签库</router-link>
      <router-link to="/selector" class="nav-link">股票标签</router-link>
    </nav>
    <router-view />
  </div>
</template>

<style>
#app {
  min-height: 100vh;
  background: #f0f2f5;
}
.navbar {
  background: #fff;
  box-shadow: 0 2px 8px rgba(0,0,0,0.08);
  padding: 0 24px;
  height: 56px;
  display: flex;
  align-items: center;
  gap: 32px;
}
.nav-brand {
  font-size: 18px;
  font-weight: 700;
  color: #409eff;
  margin-right: 16px;
}
.nav-link {
  color: #606266;
  text-decoration: none;
  font-size: 15px;
  padding: 0 8px;
  height: 56px;
  display: flex;
  align-items: center;
  border-bottom: 2px solid transparent;
  transition: all 0.2s;
}
.nav-link:hover,
.nav-link.router-link-active {
  color: #409eff;
  border-bottom-color: #409eff;
}
</style>
