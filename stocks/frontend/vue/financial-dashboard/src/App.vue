<template>
  <div id="app-container">
    <header>
      <h1>Financial Dashboard</h1>
    </header>
    <div class="layout">
      <nav class="sidebar">
        <router-link to="/stocks" class="nav-item">Stock Info</router-link>
        <router-link to="/options" class="nav-item">Option Info</router-link>
        <router-link to="/screeners" class="nav-item">Screeners</router-link>
      </nav>
      <main>
        <router-view />
      </main>
    </div>
  </div>
</template>

<script setup lang="ts">
// Router view will handle component rendering
</script>

<style>
@import '@/assets/base.css';

#app-container {
  max-width: 1600px;
  margin: 0 auto;
  padding: 0 15px;
}

header {
  background-color: #343a40;
  color: white;
  padding: 10px 20px;
  margin-bottom: 20px;
  border-radius: 5px;
}

header h1 {
  margin: 0;
  font-size: 1.5em;
}

.layout {
  display: flex;
  gap: 20px;
}

.sidebar {
  width: 200px;
  background: white;
  padding: 20px;
  border-radius: 5px;
  box-shadow: 0 2px 4px rgba(0,0,0,0.05);
  height: fit-content;
}

.nav-item {
  display: block;
  padding: 10px 15px;
  color: #333;
  text-decoration: none;
  border-radius: 5px;
  margin-bottom: 5px;
}

.nav-item:hover {
  background-color: #f0f2f5;
}

.router-link-active {
  background-color: #e2e8f0;
  font-weight: 500;
}

main {
  flex: 1;
}
</style>