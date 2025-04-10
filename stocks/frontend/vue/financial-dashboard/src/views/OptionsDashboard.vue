<template>
  <div class="options-dashboard">
    <TickerInput v-model="tickerId" @loadTicker="loadOptionsData" />

    <div v-if="loading" class="loading">Loading options data for {{ tickerId }}...</div>
    <div v-if="error" class="error">{{ error }}</div>

    <div v-if="optionsData && !loading && !error" class="dashboard-content">
      <!-- Company Info -->
      <div v-if="stockInfo" class="company-info card">
        <h2>
          {{ stockInfo.TICKER }} - {{ stockInfo.TICKER_NAME }}
          <span class="current-price">(Stock price: {{ formatCurrency(stockPrice) }})</span>
        </h2>
      </div>

      <!-- Histogram Controls -->
      <div class="histogram-controls card">
        <div class="control-group">
          <label for="days">Analysis Period:</label>
          <div class="input-with-unit">
            <input
              id="days"
              type="number"
              v-model="days"
              min="1"
              max="365"
              @change="reloadData"
            />
            <span class="unit">days</span>
          </div>
        </div>
        <div class="control-group">
          <label for="percentRange">Price Change Range:</label>
          <div class="input-with-unit">
            <input
              id="percentRange"
              type="number"
              v-model="percentRange"
              min="1"
              max="100"
              @change="reloadData"
            />
            <span class="unit">%</span>
          </div>
        </div>
      </div>

      <!-- Histogram Chart -->
      <BaseChart
        v-if="chartTraces.length > 0"
        chartId="histogram-chart"
        :title="`Price Change Distribution (${days} Days)`"
        :traces="chartTraces"
        :layoutOptions="chartLayout"
      />

      <!-- Statistics -->
      <div v-if="optionsData.statistics" class="statistics card">
        <h3>Statistics</h3>
        <div class="stats-grid">
          <div class="stat-item">
            <label>Mean Change:</label>
            <span>{{ optionsData.statistics.mean_change?.toFixed(2) }}%</span>
          </div>
          <div class="stat-item">
            <label>Standard Deviation:</label>
            <span>{{ optionsData.statistics.std_dev?.toFixed(2) }}%</span>
          </div>
          <div class="stat-item">
            <label>Min Change:</label>
            <span>{{ optionsData.statistics.min_change?.toFixed(2) }}%</span>
          </div>
          <div class="stat-item">
            <label>Max Change:</label>
            <span>{{ optionsData.statistics.max_change?.toFixed(2) }}%</span>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, watch } from 'vue';
import { useTickerStore } from '@/stores/tickerStore';
import TickerInput from '@/components/TickerInput.vue';
import BaseChart from '@/components/BaseChart.vue';
import { formatCurrency } from '@/utils/formatters';
import { getGrowthProbability } from '@/services/optionsApi';
import { getCurrentPrice, fetchStockDataById } from '@/services/stockApi';
import type { PlotlyTrace } from '@/types/stock';
import type { StockData } from '@/types/stock';

// State
const tickerStore = useTickerStore();
const tickerId = ref<string>('');
const optionsData = ref<any | null>(null);
const loading = ref<boolean>(false);
const error = ref<string | null>(null);
const stockPrice = ref<number>(0);
const stockInfo = ref<StockData | null>(null);

// Initialize histogram settings from store with defaults in case store isn't ready
const days = ref<number>(30);
const percentRange = ref<number>(20);

// Chart configuration
const chartTraces = computed<PlotlyTrace[]>(() => {
  if (!optionsData.value?.histogram) return [];

  const histogram = optionsData.value.histogram;
  const xValues = histogram.map((bin: any) => (bin.bin_start + bin.bin_end) / 2);
  const yValues = histogram.map((bin: any) => bin.count);

  // Base histogram trace
  const histogramTrace: PlotlyTrace = {
    x: xValues,
    y: yValues,
    type: 'bar',
    name: 'Frequency',
    marker: {
      color: 'rgba(54, 162, 235, 0.5)',
      line: {
        color: 'rgba(54, 162, 235, 1)',
        width: 1
      }
    }
  };

  // Add vertical lines for statistical markers
  const markerColors: { [key: string]: string } = {
    'MEAN': '#FFD700',
    '-1SD': '#FF6B6B',
    '+1SD': '#4CAF50',
    '-2SD': '#DC3545',
    '+2SD': '#28A745'
  };

  const markerTraces: PlotlyTrace[] = [];
  histogram.forEach((bin: any, index: number) => {
    bin.markers.forEach((marker: string) => {
      markerTraces.push({
        x: [xValues[index], xValues[index]],
        y: [0, Math.max(...yValues)],
        type: 'scatter',
        mode: 'lines',
        name: marker,
        line: {
          color: markerColors[marker],
          width: 2
        }
      });
    });
  });

  return [histogramTrace, ...markerTraces];
});

const chartLayout = computed(() => ({
  showlegend: true,
  xaxis: {
    title: 'Price Change (%)',
    gridcolor: '#e0e0e0',
    range: [-percentRange.value, percentRange.value], // Set range to match user input
    fixedrange: true // Prevent zooming/panning on x-axis to maintain the range
  },
  yaxis: {
    title: 'Frequency',
    gridcolor: '#e0e0e0'
  }
}));

// Methods
const loadOptionsData = async (tickerToLoad: string) => {
  loading.value = true;
  error.value = null;
  optionsData.value = null;
  
  try {
    // Load both stock price and options data
    const [stockResponse, optionsResponse] = await Promise.all([
      fetchStockDataById(tickerToLoad),
      getGrowthProbability(tickerToLoad, days.value, percentRange.value)
    ]);

    stockPrice.value = stockResponse.PRICE_DATA[1][stockResponse.PRICE_DATA[1].length - 1] || 0;
    stockInfo.value = stockResponse;
    optionsData.value = optionsResponse;
    tickerId.value = tickerToLoad;
    tickerStore.setTicker(tickerToLoad);
  } catch (err: any) {
    console.error("Error loading data:", err);
    error.value = `Failed to load data for ${tickerToLoad}. ${err.message || 'Please try again.'}`;
  } finally {
    loading.value = false;
  }
};

const reloadData = () => {
  // Update store with new values
  tickerStore.updateHistogramSettings({
    days: days.value,
    percentRange: percentRange.value
  });
  
  if (tickerId.value) {
    loadOptionsData(tickerId.value);
  }
};

// Watch for store histogram settings changes from other components
watch(() => tickerStore.histogramSettings, (newSettings) => {
  if (newSettings.days !== days.value || newSettings.percentRange !== percentRange.value) {
    days.value = newSettings.days;
    percentRange.value = newSettings.percentRange;
    loadOptionsData(tickerId.value);
  }
}, { deep: true });

// Watch for store changes from other components
watch(() => tickerStore.currentTicker, (newTicker) => {
  if (newTicker && newTicker !== tickerId.value) {
    loadOptionsData(newTicker);
  }
});

// Lifecycle
onMounted(() => {
  tickerId.value = tickerStore.currentTicker;
  days.value = tickerStore.histogramSettings?.days || 30;
  percentRange.value = tickerStore.histogramSettings?.percentRange || 20;
  
  if (tickerId.value) {
    loadOptionsData(tickerId.value);
  }
});
</script>

<style scoped>
.options-dashboard {
  padding: 20px;
  font-family: sans-serif;
  background-color: #f0f2f5;
}

.loading, .error {
  padding: 20px;
  text-align: center;
  font-size: 1.2em;
  border-radius: 5px;
  margin: 20px;
}

.loading {
  color: #007bff;
}

.error {
  color: #dc3545;
  background-color: #f8d7da;
  border: 1px solid #f5c6cb;
}

.dashboard-content {
  display: flex;
  flex-direction: column;
  gap: 20px;
}

.card {
  background-color: white;
  padding: 15px 20px;
  border-radius: 5px;
  box-shadow: 0 2px 4px rgba(0,0,0,0.05);
}

.company-info h2 {
  margin-top: 0;
  margin-bottom: 10px;
  color: #333;
  display: flex;
  align-items: baseline;
  gap: 15px;
  flex-wrap: wrap;
}

.current-price {
  font-size: 0.9em;
  font-weight: normal;
  color: #555;
}

.chart-container {
  min-height: 400px;
}

.histogram-chart {
  width: 100%;
  height: 350px;
}

.statistics h3 {
  margin-top: 0;
  margin-bottom: 15px;
}

.stats-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 15px;
}

.stat-item {
  display: flex;
  justify-content: space-between;
  padding: 8px;
  background-color: #f8f9fa;
  border-radius: 4px;
}

.stat-item label {
  color: #666;
}

.stat-item span {
  font-weight: 500;
  color: #333;
}

.histogram-controls {
  display: flex;
  gap: 20px;
  align-items: center;
  flex-wrap: wrap;
  margin-bottom: 20px;
}

.control-group {
  display: flex;
  align-items: center;
  gap: 10px;
}

.control-group label {
  font-weight: 500;
  color: #444;
  white-space: nowrap;
}

.input-with-unit {
  display: flex;
  align-items: center;
  gap: 5px;
}

.input-with-unit input {
  width: 80px;
  padding: 6px 8px;
  border: 1px solid #ccc;
  border-radius: 4px;
  font-size: 14px;
}

.input-with-unit .unit {
  color: #666;
  font-size: 14px;
}

.histogram-controls input:focus {
  outline: none;
  border-color: #007bff;
  box-shadow: 0 0 0 2px rgba(0,123,255,0.25);
}

.histogram-controls input::-webkit-inner-spin-button,
.histogram-controls input::-webkit-outer-spin-button {
  opacity: 1;
}
</style>