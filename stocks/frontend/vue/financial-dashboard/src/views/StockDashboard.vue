<template>
    <div class="stock-dashboard">
      <div class="header-controls">
        <div class="input-group">
          <TickerInput v-model="tickerId" @loadTicker="loadStockData" />
          <button 
            class="reset-button" 
            @click="showResetConfirmation" 
            :disabled="!tickerId || loading || resetLoading"
          >
            {{ resetLoading ? 'Resetting...' : 'Reset Data' }}
          </button>
        </div>
      </div>

      <!-- Reset Confirmation Dialog -->
      <div v-if="showConfirmDialog" class="dialog-overlay">
        <div class="dialog-content">
          <h3>Confirm Reset</h3>
          <p>Are you sure you want to reset data for {{ tickerId }}?<br>This action cannot be undone.</p>
          <div class="dialog-buttons">
            <button class="cancel-button" @click="showConfirmDialog = false">Cancel</button>
            <button class="confirm-button" @click="handleReset">Reset</button>
          </div>
        </div>
      </div>
  
      <div v-if="loading" class="loading">Loading data for {{ tickerId }}...</div>
      <div v-if="error" class="error">{{ error }}</div>
  
      <div v-if="stockData && !loading && !error" class="dashboard-content">
        <!-- 2. Company Info -->
        <div class="company-info card">
          <h2>
            {{ stockData.TICKER }} - {{ stockData.TICKER_NAME }}
            <span class="current-price">({{ formatCurrency(latestPrice) }})</span>
          </h2>
          <p class="description">{{ stockData.TICKER_DESCRIPTION }}</p>
        </div>
  
        <!-- Row 1: Price & Margins -->
        <div class="chart-row">
          <BaseChart
            chartId="price-chart"
            :title="`Price`"
            :traces="priceChartTraces"
            class="chart-wide"
          />
          <BaseChart
            chartId="margins-chart"
            :title="`Margins`"
            :traces="marginChartTraces"
            :layoutOptions="{ yaxis: { tickformat: '.1%' } }"
             class="chart-wide"
          />
        </div>
  
        <!-- Row 2: Valuation Charts -->
         <h2 class="section-title">Valuation Metrics</h2>
        <div class="chart-row valuation-row">
           <BaseChart
            :chartId="'pe-chart'"
            :title="`PE (${latestPE !== null ? latestPE.toFixed(2) : 'N/A'})`"
            :traces="peTrace"
            class="chart-narrow"
            :layoutOptions="{ 
                showlegend: true,
                margin: { l: 50, r: 50, b: 100, t: 50, pad: 4 },
                legend: {
                    orientation: 'h',
                    y: -0.4,
                    yanchor: 'top',
                    xanchor: 'center',
                    x: 0.5
                }
            }"
           />
           <BaseChart
            :chartId="'ps-chart'"
            :title="`PS (${latestPS !== null ? latestPS.toFixed(2) : 'N/A'})`"
            :traces="psTrace"
             class="chart-narrow"
             :layoutOptions="{ 
                showlegend: true,
                margin: { l: 50, r: 50, b: 100, t: 50, pad: 4 },
                legend: {
                    orientation: 'h',
                    y: -0.4,
                    yanchor: 'top',
                    xanchor: 'center',
                    x: 0.5
                }
            }"
          />
           <BaseChart
            :chartId="'pb-chart'"
            :title="`PB (${latestPB !== null ? latestPB.toFixed(2) : 'N/A'})`"
            :traces="pbTrace"
             class="chart-narrow"
             :layoutOptions="{ 
                showlegend: true,
                margin: { l: 50, r: 50, b: 100, t: 50, pad: 4 },
                legend: {
                    orientation: 'h',
                    y: -0.4,
                    yanchor: 'top',
                    xanchor: 'center',
                    x: 0.5
                }
            }"
          />
           <BaseChart
            :chartId="'pfcf-chart'"
            :title="`PFCF (${latestPFCF !== null ? latestPFCF.toFixed(2) : 'N/A'})`"
            :traces="pfcfTrace"
            class="chart-narrow"
            :layoutOptions="{ 
                showlegend: true,
                margin: { l: 50, r: 50, b: 100, t: 50, pad: 4 },
                legend: {
                    orientation: 'h',
                    y: -0.4,
                    yanchor: 'top',
                    xanchor: 'center',
                    x: 0.5
                }
            }"
          />
        </div>
  
        <!-- Row 3: Financial Statements & Shares -->
         <h2 class="section-title">Financials & Shares</h2>
        <div class="chart-row">
          <BaseChart
            chartId="income-statement-chart"
            title="Income statement (ttm)"
            :traces="incomeStatementTraces"
            class="chart-medium"
          />
           <BaseChart
            chartId="balance-sheet-chart"
            title="Balance sheet"
            :traces="balanceSheetTraces"
             class="chart-medium"
          />
        </div>
         <div class="chart-row">
           <BaseChart
            chartId="fcf-statement-chart"
            title="FCF Statement (ttm)"
            :traces="fcfStatementTraces"
             class="chart-medium"
          />
          <BaseChart
            :chartId="'shares-chart'"
            :title="`Shares outstanding (${sharesCAGR !== null ? sharesCAGR.toFixed(2) + '% p.a.' : 'N/A'})`"
            :traces="sharesOutstandingTrace"
            class="chart-medium"
            :layoutOptions="{ showlegend: false }"
           />
        </div>
  
      </div>
    </div>
  </template>
  
  <script setup lang="ts">
  import { ref, onMounted, computed, watch, onUnmounted } from 'vue';
  import { useTickerStore } from '@/stores/tickerStore';
  import TickerInput from '@/components/TickerInput.vue';
  import BaseChart from '@/components/BaseChart.vue';
  import { fetchStockDataById, getCurrentPrice, resetStockData } from '@/services/stockApi';
  import { calculateCAGR, estimateYears } from '@/utils/cagr';
  import { formatLargeNumber, formatPercentage, formatCurrency } from '@/utils/formatters';
  import type { StockData, ChartData, PlotlyTrace } from '@/types/stock';
  
  const tickerStore = useTickerStore();
  const tickerId = ref<string>('');
  const stockData = ref<StockData | null>(null);
  const loading = ref<boolean>(false);
  const error = ref<string | null>(null);
  const priceRefreshInterval = ref<number | null>(null);
  const resetLoading = ref<boolean>(false);
  const showConfirmDialog = ref<boolean>(false);
  
  // Initialize state once component is mounted
  onMounted(() => {
    tickerId.value = tickerStore.currentTicker;
    
    if (tickerId.value) {
      loadStockData(tickerId.value);
    }
  });
  
  // --- Helper: Get Latest Value ---
  const getLatestValue = (data: ChartData | undefined): number | null => {
    if (!data || data[1]?.length === 0) return null;
    const lastValue = data[1][data[1].length - 1];
    return typeof lastValue === 'number' ? lastValue : null;
  };
  
  // --- Helper: Create Trace ---
  const createTrace = (
      data: ChartData | undefined,
      name: string,
      options: { color?: string; yaxis?: string; type?: string; mode?: string } = {}
  ): PlotlyTrace | null => {
    if (!data || !data[0] || !data[1]) return null;
    return {
      x: data[0],
      y: data[1],
      type: options.type || 'scatter',
      mode: options.mode || 'lines',
      name: name,
      line: options.color ? { color: options.color } : undefined,
      marker: options.color ? { color: options.color } : undefined, // For scatter/bar types if needed
      yaxis: options.yaxis,
    };
  };
  
  // --- Helper: Create Trace with CAGR ---
  const createTraceWithCAGR = (
      data: ChartData | undefined,
      baseName: string,
      options: { color?: string; yaxis?: string; type?: string; mode?: string } = {}
  ): PlotlyTrace | null => {
      const trace = createTrace(data, baseName, options);
      if (!trace || !data || !data[0] || data[0].length < 2) return trace; // Return trace without CAGR if not possible
  
      const years = estimateYears(data[0]);
      const cagr = calculateCAGR(data[1], years);
      const cagrText = cagr !== null ? ` (${cagr.toFixed(2)}% p.a.)` : '';
      trace.name = `${baseName}${cagrText}`;
      return trace;
  };
  
  // --- Helper: Calculate average of values ---
  const calculateAverage = (values: (number | null)[]): number | null => {
    const validValues = values.filter((v): v is number => v !== null);
    if (validValues.length === 0) return null;
    return validValues.reduce((sum, val) => sum + val, 0) / validValues.length;
  };
  
  // --- Computed Properties for Chart Traces ---
  
  // 3. Price Chart
  const latestPrice = computed(() => getLatestValue(stockData.value?.PRICE_DATA));
  const latestAnalystTarget = computed(() => getLatestValue(stockData.value?.TARGET_PRICE_DATA));
  const latestOptionTarget = computed(() => getLatestValue(stockData.value?.OPTION_PRICE_DATA));
  const latestEpsValuation = computed(() => getLatestValue(stockData.value?.EPS_PRICE_DATA));
  const latestFcfValuation = computed(() => getLatestValue(stockData.value?.FCF_PRICE_DATA));
  
  const priceChartTraces = computed((): PlotlyTrace[] => {
    if (!stockData.value) return [];
    const traces: (PlotlyTrace | null)[] = [
      createTrace(stockData.value.PRICE_DATA, `Price (${formatCurrency(latestPrice.value)})`, { color: '#1f77b4' }), // Blue
      createTrace(stockData.value.TARGET_PRICE_DATA, `Analyst target (${formatCurrency(latestAnalystTarget.value)})`, { color: '#ff7f0e' }), // Orange
      createTrace(stockData.value.OPTION_PRICE_DATA, `Option target (${formatCurrency(latestOptionTarget.value)})`, { color: '#2ca02c' }), // Green
      createTrace(stockData.value.EPS_PRICE_DATA, `EPS valuation (${formatCurrency(latestEpsValuation.value)})`, { color: '#d62728' }), // Red
      createTrace(stockData.value.FCF_PRICE_DATA, `FCF valuation (${formatCurrency(latestFcfValuation.value)})`, { color: '#9467bd' }), // Purple
    ];
    return traces.filter((t): t is PlotlyTrace => t !== null); // Filter out null traces
  });
  
  // 4. Margin Chart
  const latestGrossMargin = computed(() => getLatestValue(stockData.value?.GROSS_MARGIN));
  const latestOperMargin = computed(() => getLatestValue(stockData.value?.OPER_MARGIN));
  const latestNetMargin = computed(() => getLatestValue(stockData.value?.NET_MARGIN));
  
  const marginChartTraces = computed((): PlotlyTrace[] => {
    if (!stockData.value) return [];
     const traces: (PlotlyTrace | null)[] = [
      createTrace(stockData.value.GROSS_MARGIN, `Gross Margin (${formatPercentage(latestGrossMargin.value, 1)})`, { color: '#1f77b4' }),
      createTrace(stockData.value.OPER_MARGIN, `Operating Margin (${formatPercentage(latestOperMargin.value, 1)})`, { color: '#ff7f0e' }),
      createTrace(stockData.value.NET_MARGIN, `Net Margin (${formatPercentage(latestNetMargin.value, 1)})`, { color: '#2ca02c' }),
     ];
     return traces.filter((t): t is PlotlyTrace => t !== null);
  });
  
  // 5. Valuation Charts
  const latestPE = computed(() => getLatestValue(stockData.value?.PE));
  const latestPS = computed(() => getLatestValue(stockData.value?.PS));
  const latestPB = computed(() => getLatestValue(stockData.value?.PB));
  const latestPFCF = computed(() => getLatestValue(stockData.value?.PFCF));
  
  const peTrace = computed((): PlotlyTrace[] => {
    if (!stockData.value?.PE) return [];
    const values = stockData.value.PE[1];
    const avgPE = calculateAverage(values);
    return [
      createTrace(stockData.value.PE, 'PE', { color: '#ff7f0e' }),
      ...(avgPE ? [
        {
          x: stockData.value.PE[0],
          y: Array(values.length).fill(avgPE),
          type: 'scatter',
          mode: 'lines',
          name: `Avg (${avgPE.toFixed(2)})`,
          line: { color: 'black', dash: 'dash', width: 2 }
        },
        {
          x: stockData.value.PE[0],
          y: Array(values.length).fill(avgPE * 1.1),
          type: 'scatter',
          mode: 'lines',
          name: `+10% (${(avgPE * 1.1).toFixed(2)})`,
          line: { color: 'black', dash: 'dash', width: 1 },
          visible: 'legendonly'
        },
        {
          x: stockData.value.PE[0],
          y: Array(values.length).fill(avgPE * 0.9),
          type: 'scatter',
          mode: 'lines',
          name: `-10% (${(avgPE * 0.9).toFixed(2)})`,
          line: { color: 'black', dash: 'dash', width: 1 },
          visible: 'legendonly'
        }
      ] : [])
    ].filter((t): t is PlotlyTrace => t !== null);
  });
  
  const psTrace = computed((): PlotlyTrace[] => {
    if (!stockData.value?.PS) return [];
    const values = stockData.value.PS[1];
    const avgPS = calculateAverage(values);
    return [
      createTrace(stockData.value.PS, 'PS', { color: '#ff7f0e' }),
      ...(avgPS ? [
        {
          x: stockData.value.PS[0],
          y: Array(values.length).fill(avgPS),
          type: 'scatter',
          mode: 'lines',
          name: `Avg (${avgPS.toFixed(2)})`,
          line: { color: 'black', dash: 'dash', width: 2 }
        },
        {
          x: stockData.value.PS[0],
          y: Array(values.length).fill(avgPS * 1.1),
          type: 'scatter',
          mode: 'lines',
          name: `+10% (${(avgPS * 1.1).toFixed(2)})`,
          line: { color: 'black', dash: 'dash', width: 1 },
          visible: 'legendonly'
        },
        {
          x: stockData.value.PS[0],
          y: Array(values.length).fill(avgPS * 0.9),
          type: 'scatter',
          mode: 'lines',
          name: `-10% (${(avgPS * 0.9).toFixed(2)})`,
          line: { color: 'black', dash: 'dash', width: 1 },
          visible: 'legendonly'
        }
      ] : [])
    ].filter((t): t is PlotlyTrace => t !== null);
  });
  
  const pbTrace = computed((): PlotlyTrace[] => {
    if (!stockData.value?.PB) return [];
    const values = stockData.value.PB[1];
    const avgPB = calculateAverage(values);
    return [
      createTrace(stockData.value.PB, 'PB', { color: '#ff7f0e' }),
      ...(avgPB ? [
        {
          x: stockData.value.PB[0],
          y: Array(values.length).fill(avgPB),
          type: 'scatter',
          mode: 'lines',
          name: `Avg (${avgPB.toFixed(2)})`,
          line: { color: 'black', dash: 'dash', width: 2 }
        },
        {
          x: stockData.value.PB[0],
          y: Array(values.length).fill(avgPB * 1.1),
          type: 'scatter',
          mode: 'lines',
          name: `+10% (${(avgPB * 1.1).toFixed(2)})`,
          line: { color: 'black', dash: 'dash', width: 1 },
          visible: 'legendonly'
        },
        {
          x: stockData.value.PB[0],
          y: Array(values.length).fill(avgPB * 0.9),
          type: 'scatter',
          mode: 'lines',
          name: `-10% (${(avgPB * 0.9).toFixed(2)})`,
          line: { color: 'black', dash: 'dash', width: 1 },
          visible: 'legendonly'
        }
      ] : [])
    ].filter((t): t is PlotlyTrace => t !== null);
  });
  
  const pfcfTrace = computed((): PlotlyTrace[] => {
    if (!stockData.value?.PFCF) return [];
    const values = stockData.value.PFCF[1];
    const avgPFCF = calculateAverage(values);
    return [
      createTrace(stockData.value.PFCF, 'PFCF', { color: '#ff7f0e' }),
      ...(avgPFCF ? [
        {
          x: stockData.value.PFCF[0],
          y: Array(values.length).fill(avgPFCF),
          type: 'scatter',
          mode: 'lines',
          name: `Avg (${avgPFCF.toFixed(2)})`,
          line: { color: 'black', dash: 'dash', width: 2 }
        },
        {
          x: stockData.value.PFCF[0],
          y: Array(values.length).fill(avgPFCF * 1.1),
          type: 'scatter',
          mode: 'lines',
          name: `+10% (${(avgPFCF * 1.1).toFixed(2)})`,
          line: { color: 'black', dash: 'dash', width: 1 },
          visible: 'legendonly'
        },
        {
          x: stockData.value.PFCF[0],
          y: Array(values.length).fill(avgPFCF * 0.9),
          type: 'scatter',
          mode: 'lines',
          name: `-10% (${(avgPFCF * 0.9).toFixed(2)})`,
          line: { color: 'black', dash: 'dash', width: 1 },
          visible: 'legendonly'
        }
      ] : [])
    ].filter((t): t is PlotlyTrace => t !== null);
  });
  
  
  // 6. Income Statement Chart
  const incomeStatementTraces = computed((): PlotlyTrace[] => {
    if (!stockData.value) return [];
    const traces: (PlotlyTrace | null)[] = [
      createTraceWithCAGR(stockData.value.REVENUE, 'Revenue', { color: '#1f77b4' }),
      createTraceWithCAGR(stockData.value.GROSS_PROFIT, 'Gross Profit', { color: '#ff7f0e' }),
      createTraceWithCAGR(stockData.value.EBITDA, 'EBITDA', { color: '#2ca02c' }),
      createTraceWithCAGR(stockData.value.NET_INCOME, 'Net Income', { color: '#d62728' }),
    ];
    return traces.filter((t): t is PlotlyTrace => t !== null);
  });
  
  // 7. Balance Sheet Chart
  const balanceSheetTraces = computed((): PlotlyTrace[] => {
    if (!stockData.value) return [];
    const traces: (PlotlyTrace | null)[] = [
      createTraceWithCAGR(stockData.value.ASSETS, 'Assets', { color: '#1f77b4' }),
      createTraceWithCAGR(stockData.value.LIABILITIES, 'Liabilities', { color: '#ff7f0e' }),
      createTraceWithCAGR(stockData.value.EQUITY, 'Equity', { color: '#2ca02c' }),
      createTraceWithCAGR(stockData.value.LONG_TERM_DEBT, 'Long debt', { color: '#9467bd' }),
      createTraceWithCAGR(stockData.value.CASH, 'Cash', { color: '#ffbb78' }), // Light orange
    ];
     return traces.filter((t): t is PlotlyTrace => t !== null);
  });
  
  // 8. FCF Statement Chart
  const fcfStatementTraces = computed((): PlotlyTrace[] => {
    if (!stockData.value) return [];
    const traces: (PlotlyTrace | null)[] = [
      createTraceWithCAGR(stockData.value.OPER_FCF, 'FCF Op', { color: '#1f77b4' }),
      createTraceWithCAGR(stockData.value.FCF, 'FCF', { color: '#d62728' }),
    ];
    return traces.filter((t): t is PlotlyTrace => t !== null);
  });
  
  // 9. Shares Outstanding Chart
  const sharesCAGR = computed(() => {
      if (!stockData.value?.SHARES_OUTSTANDING) return null;
      const years = estimateYears(stockData.value.SHARES_OUTSTANDING[0]);
      return calculateCAGR(stockData.value.SHARES_OUTSTANDING[1], years);
  });
  const sharesOutstandingTrace = computed((): PlotlyTrace[] => stockData.value?.SHARES_OUTSTANDING
      ? [createTrace(stockData.value.SHARES_OUTSTANDING, 'Shares Outstanding', { color: '#ff7f0e' })].filter((t): t is PlotlyTrace => t !== null)
      : []);
  
  // --- Methods ---
  const refreshCurrentPrice = async () => {
    if (!tickerId.value) return;
    
    try {
      const priceData = await getCurrentPrice(tickerId.value);
      if (stockData.value) {
        // Update the last price in the price data array
        stockData.value.PRICE_DATA[1][stockData.value.PRICE_DATA[1].length - 1] = priceData.price;
      }
    } catch (err) {
      console.error('Error refreshing price:', err);
    }
  };

  const loadStockData = async (tickerToLoad: string) => {
    loading.value = true;
    error.value = null;
    stockData.value = null; // Clear previous data
    try {
      console.log(`Fetching data for: ${tickerToLoad}`);
      const [stockResponse, currentPrice] = await Promise.all([
        fetchStockDataById(tickerToLoad),
        getCurrentPrice(tickerToLoad)
      ]);
  
      // Update the last price with current price
      if (stockResponse.PRICE_DATA[1].length > 0) {
        stockResponse.PRICE_DATA[1][stockResponse.PRICE_DATA[1].length - 1] = currentPrice.price;
      }
  
      stockData.value = stockResponse;
      tickerId.value = tickerToLoad; // Update tickerId only on success
      tickerStore.setTicker(tickerToLoad); // Update shared store
  
      // Setup price refresh interval
      if (priceRefreshInterval.value) {
        clearInterval(priceRefreshInterval.value);
      }
      priceRefreshInterval.value = window.setInterval(refreshCurrentPrice, 3000);
  
      console.log(`Data received for: ${tickerToLoad}`);
    } catch (err: any) {
      console.error("Error in loadStockData:", err);
      error.value = `Failed to load data for ${tickerToLoad}. ${err.message || 'Please try again.'}`;
      stockData.value = null; // Ensure data is null on error
    } finally {
      loading.value = false;
    }
  };

  const showResetConfirmation = () => {
    showConfirmDialog.value = true;
  };

  const handleReset = async () => {
    if (!tickerId.value || resetLoading.value) return;
    
    showConfirmDialog.value = false; // Close dialog
    resetLoading.value = true;
    error.value = null;
    
    try {
      await resetStockData(tickerId.value);
      // Reload the data after reset
      await loadStockData(tickerId.value);
    } catch (err: any) {
      error.value = `Failed to reset data: ${err.message}`;
    } finally {
      resetLoading.value = false;
    }
  };
  
  // Watch for store changes from other components
  watch(() => tickerStore.currentTicker, (newTicker) => {
    if (newTicker && newTicker !== tickerId.value) {
      loadStockData(newTicker);
    }
  });
  
  // --- Lifecycle Hooks ---
  onMounted(() => {
    if (tickerId.value) {
      loadStockData(tickerId.value);
    }
  });

  onUnmounted(() => {
    if (priceRefreshInterval.value) {
      clearInterval(priceRefreshInterval.value);
    }
  });
  
  // Optional: Watch tickerId if you want loading to trigger just by changing the v-model
  // watch(tickerId, (newTicker) => {
  //   if (newTicker) {
  //      loadStockData(newTicker);
  //   }
  // });
  
  </script>
  
  <style scoped>
  .stock-dashboard {
    padding: 20px;
    font-family: sans-serif;
    background-color: #f0f2f5; /* Light background for the whole page */
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
    gap: 20px; /* Spacing between sections/rows */
  }
  
  .card {
     background-color: white;
     padding: 15px 20px;
     border-radius: 5px;
     box-shadow: 0 2px 4px rgba(0,0,0,0.05);
     margin-bottom: 20px; /* Space below info */
  }
  
  .company-info h2 {
    margin-top: 0;
    margin-bottom: 10px;
    color: #333;
    display: flex;
    align-items: baseline;
    gap: 15px;
    flex-wrap: wrap; /* Allow wrapping if needed */
  }
  .company-info .current-price {
    font-size: 0.9em;
    font-weight: normal;
    color: #555;
  }
  .company-info .description {
    font-size: 0.9em;
    color: #666;
    line-height: 1.5;
  }
  
  .section-title {
      margin-top: 30px;
      margin-bottom: 10px;
      color: #444;
      border-bottom: 1px solid #ddd;
      padding-bottom: 5px;
  }
  
  .chart-row {
    display: flex;
    flex-wrap: wrap; /* Allow charts to wrap on smaller screens */
    gap: 20px; /* Spacing between charts in a row */
    justify-content: space-between; /* Distribute space */
  }
  
  /* Adjust chart widths based on layout */
  .chart-wide {
    flex: 1 1 calc(50% - 10px); /* Try to take half width, minus gap */
    min-width: 350px; /* Minimum width before wrapping */
  }
  
  .chart-medium {
     flex: 1 1 calc(50% - 10px);
     min-width: 350px;
  }
  
  .chart-narrow {
    flex: 1 1 calc(25% - 15px); /* Try to take quarter width, minus gap */
     min-width: 250px;
  }
  
  /* Responsive adjustments */
  @media (max-width: 1200px) {
      .chart-narrow {
           flex-basis: calc(50% - 10px); /* Two per row on medium screens */
      }
      .chart-wide, .chart-medium {
          flex-basis: calc(100% - 10px); /* Full width on medium screens */
      }
  }
  
  @media (max-width: 768px) {
    .chart-row {
      flex-direction: column; /* Stack charts vertically on small screens */
    }
    .chart-wide, .chart-medium, .chart-narrow {
       flex-basis: 100%; /* Full width when stacked */
       min-width: unset;
    }
    .ticker-input {
        flex-direction: column;
        align-items: stretch;
    }
  }

  .header-controls {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 0;
  }

  .input-group {
    display: flex;
    align-items: stretch;
    gap: 10px;
    flex-wrap: nowrap;
  }
  
  .reset-button {
    padding: 8px 15px;
    background-color: #dc3545;
    color: white;
    border: none;
    border-radius: 4px;
    cursor: pointer;
    transition: background-color 0.2s;
    min-width: 100px;
    height: 52px; /* Match TickerInput total height (36px + 2*8px padding) */
    box-sizing: border-box;
  }
  
  .reset-button:hover:not(:disabled) {
    background-color: #c82333;
  }
  
  .reset-button:disabled {
    background-color: #6c757d;
    cursor: not-allowed;
    opacity: 0.65;
  }

  .dialog-overlay {
    position: fixed;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background-color: rgba(0, 0, 0, 0.5);
    display: flex;
    justify-content: center;
    align-items: center;
    z-index: 1000;
  }
  
  .dialog-content {
    background: white;
    padding: 20px;
    border-radius: 8px;
    min-width: 300px;
    box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);
  }
  
  .dialog-content h3 {
    margin-top: 0;
    color: #333;
  }
  
  .dialog-content p {
    color: #666;
    margin: 15px 0;
  }
  
  .dialog-buttons {
    display: flex;
    justify-content: flex-end;
    gap: 10px;
    margin-top: 20px;
  }
  
  .cancel-button {
    padding: 8px 15px;
    background-color: #6c757d;
    color: white;
    border: none;
    border-radius: 4px;
    cursor: pointer;
  }
  
  .cancel-button:hover {
    background-color: #5a6268;
  }
  
  .confirm-button {
    padding: 8px 15px;
    background-color: #dc3545;
    color: white;
    border: none;
    border-radius: 4px;
    cursor: pointer;
  }
  
  .confirm-button:hover {
    background-color: #c82333;
  }
  
  </style>