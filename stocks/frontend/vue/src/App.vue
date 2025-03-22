<template>
  <div id="app">
    <h1>Stock Data</h1>
    <StockTable title="Top 10 by Market Cap" :stocks="topMarketCap" />
    <StockTable title="Top 10 by Price Prob & Recomm" :stocks="topPriceProbRecomm" />
    <StockTable title="Top 10 by Price Target Diff" :stocks="topPriceTargetDiff" />
  </div>
</template>

<script>
import { defineComponent, ref, onMounted } from 'vue';
import StockTable from './components/StockTable.vue';
import axios from 'axios';

export default defineComponent({
  name: 'App',
  components: {
    StockTable
  },
  setup() {
    const topMarketCap = ref([]);
    const topPriceProbRecomm = ref([]);
    const topPriceTargetDiff = ref([]);

    const limit = 20;
    const defaultWhereSection = " target_price is not NULL and target_price > price and pe>0 and predict_eps_cagr > 0 and recomm_mean < 2 and growth_rate > 0 and isin like '%US%' and pe between 0 and 70 and growth_rate_stability > 2 "

    onMounted(async () => {
      const marketCapResponse = await axios.post('/api/query', { query: 'SELECT * FROM tickers WHERE ' + defaultWhereSection + ' ORDER BY market_cap DESC LIMIT ' + limit });
      topMarketCap.value = marketCapResponse.data.data;

      const priceProbRecommResponse = await axios.post('/api/query', { query: 'SELECT * FROM tickers WHERE ' + defaultWhereSection + ' AND price_prob_1 is not null ORDER BY ABS(price_prob_1) ASC LIMIT ' + limit });
      topPriceProbRecomm.value = priceProbRecommResponse.data.data;

      const priceTargetDiffResponse = await axios.post('/api/query', { query: 'SELECT *, (target_price - price) / price AS price_diff FROM tickers where  ' + defaultWhereSection + '  ORDER BY price_diff DESC LIMIT ' + limit });
      topPriceTargetDiff.value = priceTargetDiffResponse.data.data;
    });

    return {
      topMarketCap,
      topPriceProbRecomm,
      topPriceTargetDiff
    };
  }
});
</script>

<style>
#app {
  font-family: Avenir, Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-align: center;
  color: #2c3e50;
  margin-top: 60px;
}
</style>
