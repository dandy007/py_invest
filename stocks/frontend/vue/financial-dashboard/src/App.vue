<template>
  <div id="app-container">
    <header>
      <h1>Stock Dashboard</h1>
    </header>
    <main>
      <StockDashboard />
    </main>
  </div>
</template>

<script setup lang="ts">
import StockDashboard from './views/StockDashboard.vue';
</script>

<style>
/* Import base styles or define global styles here */
@import '@/assets/base.css'; /* If you have global styles */

#app-container {
  max-width: 1600px; /* Max width for the dashboard */
  margin: 0 auto;   /* Center the container */
  padding: 0 15px; /* Padding on the sides */
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

main {
  /* Styles for the main content area */
}
</style>