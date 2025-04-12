<template>
  <div class="chart-container">
    <h3>{{ title }}</h3>
    <div :id="chartId" class="plotly-chart"></div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted, watch, nextTick } from 'vue';
import * as Plotly from 'plotly.js'; // Fixed import statement
import type { Data, Layout } from 'plotly.js';
import type { PlotlyTrace } from '@/types/stock';

const props = defineProps<{
  chartId: string; // Unique ID for the chart div
  title: string;
  traces: PlotlyTrace[];
  layoutOptions?: Partial<Layout>; // Allow overriding parts of the layout
}>();

const chartElement = ref<HTMLElement | null>(null); // Ref for the div

const defaultLayout: Partial<Layout> = {
  autosize: true,
  margin: { l: 50, r: 50, b: 50, t: 50, pad: 4 }, // Adjust margins
  xaxis: {
    type: 'date',
    // automargin: true,
     gridcolor: '#e0e0e0',
  },
  yaxis: {
    // automargin: true,
     gridcolor: '#e0e0e0',
     zerolinecolor: '#c0c0c0',
     zerolinewidth: 1,
  },
  yaxis2: { // Example for potential second axis
    overlaying: 'y',
    side: 'right',
    showgrid: false,
    zeroline: false,
  },
  legend: {
    orientation: 'h', // Horizontal legend below chart
    yanchor: 'bottom',
    y: -0.3, // Adjust position below x-axis
    xanchor: 'center',
    x: 0.5
  },
  hovermode: 'x unified', // Show tooltips for all traces at a given x
  paper_bgcolor: '#ffffff', // Background of the plotting area
  plot_bgcolor: '#f8f8f8',  // Background of the chart itself
};

const renderChart = async () => {
  // Ensure the element is available in the DOM
  await nextTick();
  const element = document.getElementById(props.chartId);
  if (element && props.traces && props.traces.length > 0) {
    const finalLayout = { ...defaultLayout, ...props.layoutOptions, title: undefined }; // Don't use Plotly's title

    // Use react for efficient updates
    Plotly.react(element, props.traces as Data[], finalLayout, { responsive: true });
  } else if (element) {
      // Clear chart if no traces
      Plotly.purge(element);
  }
};

onMounted(() => {
  renderChart();
});

// Watch for changes in traces or layout options to re-render
watch(() => [props.traces, props.layoutOptions], () => {
  renderChart();
}, { deep: true }); // Deep watch might be needed depending on how traces/layout change


// Optional: Handle window resize for better responsiveness if needed,
// though Plotly.react with { responsive: true } often handles it well.
// const handleResize = () => {
//   const element = document.getElementById(props.chartId);
//   if (element) {
//     Plotly.Plots.resize(element);
//   }
// };
// onMounted(() => window.addEventListener('resize', handleResize));
// onUnmounted(() => window.removeEventListener('resize', handleResize));

</script>

<style scoped>
.chart-container {
  border: 1px solid #eee;
  padding: 15px;
  margin-bottom: 20px;
  background-color: white;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
  border-radius: 5px;
  min-height: 650px; /* Zvýšeno z 350px pro zachování poměru s obsahem */
  display: flex;
  flex-direction: column;
}
.chart-container h3 {
  margin-top: 0;
  margin-bottom: 10px;
  text-align: center;
  font-size: 1.1em;
  color: #333;
}
.plotly-chart {
  width: 100%;
  flex-grow: 1; /* Allow chart div to fill container */
  min-height: 600px; /* Zvýšeno z 300px na dvojnásobek */
}
</style>