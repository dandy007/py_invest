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

      <!-- Options Chain Section -->
      <div class="options-chain card">
        <div class="expiration-selector">
          <label for="expiration">Expiration Date:</label>
          <select 
            id="expiration" 
            v-model="selectedExpiration"
            @change="handleExpirationChange"
          >
            <option value="">Select expiration</option>
            <option v-for="date in expirationDates" :key="date" :value="date">
              {{ formatDate(date) }} ({{ getDaysUntil(date) }} days)
            </option>
          </select>
        </div>

        <div class="chain-tables" v-if="selectedExpiration">
          <!-- Call Options Table -->
          <div class="chain-table">
            <div class="table-header">
              <h3>Call Options</h3>
              <div class="current-price-indicator">Current Price: {{ formatCurrency(stockPrice) }}</div>
            </div>
            <div class="table-container" ref="callTableRef" @scroll="handleCallScroll">
              <table>
                <thead>
                  <tr>
                    <th>Strike</th>
                    <th>Premium %</th>
                    <th>Last</th>
                    <th>Bid</th>
                    <th>Ask</th>
                    <th>Volume</th>
                    <th>OI</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="(option, index) in callChain" 
                    :key="option.strike"
                    :class="{
                      'alternate-row': index % 2 === 0,
                      'near-strike': isNearestStrike(option.strike),
                      'sd-one': isWithinStandardDeviation(option.strike, 1),
                      'sd-two': isWithinStandardDeviation(option.strike, 2)
                    }">
                    <td>{{ formatCurrency(option.strike) }}</td>
                    <td>{{ calculatePremiumPercent(option.bid, option.strike) }}%</td>
                    <td>{{ formatCurrency(option.last_price) }}</td>
                    <td>{{ formatCurrency(option.bid) }}</td>
                    <td>{{ formatCurrency(option.ask) }}</td>
                    <td>{{ formatNumber(option.volume) }}</td>
                    <td>{{ formatNumber(option.open_interest) }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>

          <!-- Put Options Table -->
          <div class="chain-table">
            <div class="table-header">
              <h3>Put Options</h3>
              <div class="current-price-indicator">Current Price: {{ formatCurrency(stockPrice) }}</div>
            </div>
            <div class="table-container" ref="putTableRef" @scroll="handlePutScroll">
              <table>
                <thead>
                  <tr>
                    <th>Strike</th>
                    <th>Premium %</th>
                    <th>Last</th>
                    <th>Bid</th>
                    <th>Ask</th>
                    <th>Volume</th>
                    <th>OI</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="(option, index) in putChain" 
                    :key="option.strike"
                    :class="{
                      'alternate-row': index % 2 === 0,
                      'near-strike': isNearestStrike(option.strike),
                      'sd-one': isWithinStandardDeviation(option.strike, 1),
                      'sd-two': isWithinStandardDeviation(option.strike, 2)
                    }">
                    <td>{{ formatCurrency(option.strike) }}</td>
                    <td>{{ calculatePremiumPercent(option.bid, option.strike) }}%</td>
                    <td>{{ formatCurrency(option.last_price) }}</td>
                    <td>{{ formatCurrency(option.bid) }}</td>
                    <td>{{ formatCurrency(option.ask) }}</td>
                    <td>{{ formatNumber(option.volume) }}</td>
                    <td>{{ formatNumber(option.open_interest) }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>

      <!-- Add PmccAnalysis component after the options chain tables -->
      <PmccAnalysis :tickerId="tickerId" />
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, watch, onUnmounted } from 'vue';
import { useTickerStore } from '@/stores/tickerStore';
import TickerInput from '@/components/TickerInput.vue';
import BaseChart from '@/components/BaseChart.vue';
import { formatCurrency } from '@/utils/formatters';
import { getGrowthProbability, getOptionChain, getExpirationDates } from '@/services/optionsApi';
import type { OptionChainItem } from '@/services/optionsApi';
import { getCurrentPrice, fetchStockDataById } from '@/services/stockApi';
import type { PlotlyTrace } from '@/types/stock';
import type { StockData } from '@/types/stock';
import PmccAnalysis from '@/components/PmccAnalysis.vue';

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

// Add these to the existing state section
const priceRefreshInterval = ref<number | null>(null);
const chainRefreshInterval = ref<number | null>(null);

// Add these refs for table synchronization
const callTableRef = ref<HTMLElement | null>(null);
const putTableRef = ref<HTMLElement | null>(null);
const isManualScroll = ref(false);

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
    // Reset chains before loading new data
    callChain.value = [];
    putChain.value = [];
    selectedExpiration.value = '';
    
    // Load initial data
    const [stockResponse, optionsResponse, currentPrice] = await Promise.all([
      fetchStockDataById(tickerToLoad),
      getGrowthProbability(tickerToLoad, days.value, percentRange.value),
      getCurrentPrice(tickerToLoad)
    ]);

    stockPrice.value = currentPrice.price;
    stockInfo.value = stockResponse;
    optionsData.value = optionsResponse;
    tickerId.value = tickerToLoad;
    tickerStore.setTicker(tickerToLoad);

    // Setup price refresh interval
    if (priceRefreshInterval.value) {
      clearInterval(priceRefreshInterval.value);
    }
    priceRefreshInterval.value = window.setInterval(refreshCurrentPrice, 3000);

    // Load expiration dates and chains after basic data is loaded
    await loadExpirationDates();
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

// Add cleanup on component unmount
onUnmounted(() => {
  if (priceRefreshInterval.value) {
    clearInterval(priceRefreshInterval.value);
  }
  if (chainRefreshInterval.value) {
    clearInterval(chainRefreshInterval.value);
  }
});

// Load expiration dates
const expirationDates = ref<string[]>([]);
const selectedExpiration = ref('');
const callChain = ref<OptionChainItem[]>([]);
const putChain = ref<OptionChainItem[]>([]);

const formatDate = (date: string) => {
  return new Date(date).toLocaleDateString();
};

const formatNumber = (num: number) => {
  return num.toLocaleString();
};

// Upravíme loadExpirationDates
const loadExpirationDates = async () => {
  if (!tickerId.value) return;
  
  try {
    const dates = await getExpirationDates(tickerId.value);
    expirationDates.value = dates;
    
    // Vybereme první datum pouze pokud není již nějaká expirace vybraná
    if (expirationDates.value.length > 0 && !selectedExpiration.value) {
      selectedExpiration.value = expirationDates.value[0];
      await loadOptionChains();
    }
  } catch (err) {
    console.error('Failed to load expiration dates:', err);
  }
};

const loadOptionChains = async () => {
  if (!tickerId.value || !selectedExpiration.value) return;

  try {
    const [callData, putData] = await Promise.all([
      getOptionChain(tickerId.value, selectedExpiration.value, 'call'),
      getOptionChain(tickerId.value, selectedExpiration.value, 'put')
    ]);

    callChain.value = callData.option_chain;
    putChain.value = putData.option_chain;

    // Nastavíme interval pro refresh dat
    if (chainRefreshInterval.value) {
      clearInterval(chainRefreshInterval.value);
    }
    chainRefreshInterval.value = window.setInterval(refreshOptionChains, 3000);
  } catch (err) {
    console.error('Failed to load option chains:', err);
  }
};

const hoveredRow = ref<string | null>(null);

// Function to determine if a strike price is the nearest to current stock price
const nearestStrike = computed(() => {
  const allStrikes = [...callChain.value, ...putChain.value].map(opt => opt.strike);
  if (!allStrikes.length) return null;
  
  return allStrikes.reduce((prev, curr) => {
    return Math.abs(curr - stockPrice.value) < Math.abs(prev - stockPrice.value) ? curr : prev;
  }, allStrikes[0]);
});

const isNearestStrike = (strike: number) => {
  return strike === nearestStrike.value;
};

// Add this new method
const refreshCurrentPrice = async () => {
  if (!tickerId.value) return;
  
  try {
    const priceData = await getCurrentPrice(tickerId.value);
    stockPrice.value = priceData.price;
  } catch (err) {
    console.error('Error refreshing price:', err);
  }
};

// Upravíme watch na selectedExpiration - odstraníme interval
watch(selectedExpiration, async (newExpiration, oldExpiration) => {
  if (newExpiration && newExpiration !== oldExpiration) {
    await loadOptionChains();
  }
});

// Add watch for expiration changes to handle interval
watch(selectedExpiration, (newExpiration) => {
  if (newExpiration) {
    loadOptionChains();
  }
});

// Add getDaysUntil function after other functions
const getDaysUntil = (dateStr: string): number => {
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const expDate = new Date(dateStr);
  expDate.setHours(0, 0, 0, 0);
  const diffTime = expDate.getTime() - today.getTime();
  return Math.ceil(diffTime / (1000 * 60 * 60 * 24));
};

// Add handleExpirationChange method
const handleExpirationChange = async () => {
  if (selectedExpiration.value) {
    // Update histogram days to match expiration
    const daysToExp = getDaysUntil(selectedExpiration.value);
    days.value = daysToExp;
    
    // Update store and reload data
    tickerStore.updateHistogramSettings({
      days: daysToExp,
      percentRange: percentRange.value
    });
    
    // Reload both histogram and option chains
    await Promise.all([
      loadOptionsData(tickerId.value),
      loadOptionChains()
    ]);
  }
};

// Add the standard deviation check function
const isWithinStandardDeviation = (strike: number, sdLevel: number) => {
  if (!optionsData.value?.statistics || !stockPrice.value) return false;
  
  const { mean_change, std_dev } = optionsData.value.statistics;
  if (mean_change === undefined || std_dev === undefined) return false;

  const meanPrice = stockPrice.value * (1 + mean_change / 100);
  const sdAmount = (stockPrice.value * std_dev / 100) * sdLevel;
  
  const lowerBound = meanPrice - sdAmount;
  const upperBound = meanPrice + sdAmount;

  return strike >= lowerBound && strike <= upperBound && 
         !(sdLevel === 2 && isWithinStandardDeviation(strike, 1)); // Exclude 1SD range from 2SD
};

// Přidáme watch na stockPrice pro vynucení přepočítání zvýraznění
watch(stockPrice, async () => {
  // Přepočítáme histogram data pro aktualizaci SD rozsahů
  if (tickerId.value) {
    const optionsResponse = await getGrowthProbability(tickerId.value, days.value, percentRange.value);
    optionsData.value = optionsResponse;
  }
});

// Add premium calculation function
const calculatePremiumPercent = (bid: number, strike: number): string => {
  if (!bid || !stockPrice.value) return '0.00';
  return ((bid / stockPrice.value) * 100).toFixed(2);
};

// Přidáme refreshOptionChains funkci zpět
const refreshOptionChains = async () => {
  if (!tickerId.value || !selectedExpiration.value) return;

  try {
    const [callData, putData] = await Promise.all([
      getOptionChain(tickerId.value, selectedExpiration.value, 'call'),
      getOptionChain(tickerId.value, selectedExpiration.value, 'put')
    ]);

    // Aktualizujeme pouze hodnoty pro existující strike ceny
    callChain.value = callChain.value.map(existingOption => {
      const updatedOption = callData.option_chain.find(o => o.strike === existingOption.strike);
      return updatedOption || existingOption;
    });

    putChain.value = putChain.value.map(existingOption => {
      const updatedOption = putData.option_chain.find(o => o.strike === existingOption.strike);
      return updatedOption || existingOption;
    });
  } catch (err) {
    console.error('Error refreshing option chains:', err);
  }
};

// Add these methods for table scroll synchronization
const handleCallScroll = (event: Event) => {
  if (!isManualScroll.value && putTableRef.value) {
    isManualScroll.value = true;
    putTableRef.value.scrollTop = (event.target as HTMLElement).scrollTop;
    setTimeout(() => {
      isManualScroll.value = false;
    }, 50);
  }
};

const handlePutScroll = (event: Event) => {
  if (!isManualScroll.value && callTableRef.value) {
    isManualScroll.value = true;
    callTableRef.value.scrollTop = (event.target as HTMLElement).scrollTop;
    setTimeout(() => {
      isManualScroll.value = false;
    }, 50);
  }
};
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

.options-chain {
  margin-top: 20px;
}

.expiration-selector {
  margin-bottom: 20px;
}

.expiration-selector label {
  margin-right: 10px;
  font-weight: 500;
}

.expiration-selector select {
  padding: 8px;
  border: 1px solid #ccc;
  border-radius: 4px;
  min-width: 200px;
  font-size: 14px;
}

.chain-tables {
  display: flex;
  gap: 20px;
  justify-content: space-between;
}

.chain-table {
  flex: 1;
  min-width: 0;
  position: relative;
  display: flex;
  flex-direction: column;
  background: white;
}

.table-header {
  position: sticky;
  top: 0;
  z-index: 2;
  background: white;
  padding: 10px;
  border-bottom: 2px solid #eee;
}

.table-header h3 {
  margin: 0 0 10px 0;
  color: #333;
}

.current-price-indicator {
  padding: 8px;
  background-color: #bbdefb;
  border-radius: 4px;
  font-weight: 500;
  color: #0d47a1;
  text-align: center;
}

.table-container {
  height: 500px;
  overflow-y: auto;
  border: 1px solid #eee;
  border-radius: 4px;
}

.chain-table table {
  width: 100%;
  border-collapse: collapse;
  font-size: 14px;
}

.chain-table th,
.chain-table td {
  padding: 8px;
  text-align: right;
  border-bottom: 1px solid #eee;
}

.chain-table th {
  position: sticky;
  top: 0;
  z-index: 1;
  background: white;
  font-weight: 500;
  color: #666;
  border-bottom: 2px solid #ddd;
}

/* Ensure both tables show exact same number of rows */
.table-container {
  scrollbar-gutter: stable;
}

/* Make scrollbars consistent across browsers */
.table-container::-webkit-scrollbar {
  width: 10px;
}

.table-container::-webkit-scrollbar-track {
  background: #f1f1f1;
  border-radius: 5px;
}

.table-container::-webkit-scrollbar-thumb {
  background: #888;
  border-radius: 5px;
}

.table-container::-webkit-scrollbar-thumb:hover {
  background: #666;
}

.chain-table tbody tr {
  transition: background-color 0.2s;
}

.chain-table tbody tr.alternate-row {
  background-color: rgba(0, 0, 0, 0.05);
}

.chain-table tbody tr.near-strike {
  background-color: #c8e6c9 !important; /* Výraznější zelená barva pro nejbližší strike */
  font-weight: 500;
}

.chain-table tbody tr:hover {
  background-color: rgba(25, 118, 210, 0.15) !important;
}

/* Responsive adjustments */
@media (max-width: 1200px) {
  .chain-tables {
    flex-direction: column;
  }
  
  .chain-table {
    margin-bottom: 20px;
  }
}

/* Modify CSS for strike price highlighting based on standard deviations */
.chain-table tbody tr.sd-one:not(.near-strike) {
  background-color: #ffeeba !important;
}

.chain-table tbody tr.sd-two:not(.near-strike) {
  background-color: #f5c6cb !important;
}
</style>