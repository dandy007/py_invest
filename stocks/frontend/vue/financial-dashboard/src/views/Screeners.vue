<template>
  <div class="screeners">
    <h2>Stock Screeners</h2>
    <div class="screeners-grid">
      <!-- Value Stocks Card -->
      <div class="screener-card">
        <h3>Value Stocks</h3>
        <div class="metrics">
          <div class="metric">
            <p>P/E < 15</p>
          </div>
          <div class="metric">
            <p>P/B < 1.5</p>
          </div>
          <div class="metric">
            <p>Dividend Yield > 2%</p>
          </div>
        </div>
        <button class="screen-button">Run Screener</button>
      </div>

      <!-- Growth Stocks Card -->
      <div class="screener-card">
        <h3>Growth Stocks</h3>
        <div class="metrics">
          <div class="metric">
            <p>Revenue Growth > 15%</p>
          </div>
          <div class="metric">
            <p>EPS Growth > 15%</p>
          </div>
          <div class="metric">
            <p>Profit Margin > 10%</p>
          </div>
        </div>
        <button class="screen-button">Run Screener</button>
      </div>

      <!-- Quality Stocks Card -->
      <div class="screener-card">
        <h3>Quality Stocks</h3>
        <div class="metrics">
          <div class="metric">
            <p>ROE > 15%</p>
          </div>
          <div class="metric">
            <p>Debt/Equity < 1</p>
          </div>
          <div class="metric">
            <p>Operating Margin > 15%</p>
          </div>
        </div>
        <button class="screen-button">Run Screener</button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
// Screener logic will be implemented later
</script>

<style scoped>
.screeners {
  padding: 20px;
}

.screeners h2 {
  margin-bottom: 20px;
  color: #333;
}

.screeners-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
  gap: 20px;
}

.screener-card {
  background: white;
  border-radius: 8px;
  padding: 20px;
  box-shadow: 0 2px 4px rgba(0,0,0,0.1);
  display: flex;
  flex-direction: column;
}

.screener-card h3 {
  margin: 0 0 15px 0;
  color: #2c3e50;
}

.metrics {
  flex-grow: 1;
  margin-bottom: 15px;
}

.metric {
  background: #f8f9fa;
  padding: 10px;
  margin-bottom: 8px;
  border-radius: 4px;
}

.metric p {
  margin: 0;
  color: #495057;
}

.screen-button {
  background: #007bff;
  color: white;
  border: none;
  padding: 10px;
  border-radius: 4px;
  cursor: pointer;
  transition: background-color 0.2s;
}

.screen-button:hover {
  background: #0056b3;
}
</style>