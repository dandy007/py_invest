<template>
  <div>
    <h2>{{ title }}</h2>
    <table>
      <thead>
        <tr>
          <th>Ticker</th>
          <th>Name</th>
          <th>PE</th>
          <th>Price</th>
          <th>Target Price</th>
          <th>Market Cap</th>
          <th>Price Prob 1</th>
          <th>Recomm Mean</th>
          <th>Growth Rate Stab.</th>
          <th>Predict EPS CAGR</th>
          <th>ATH Discount</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="stock in stocks" :key="stock.ticker_id">
          <td>{{ stock.ticker_id }}</td>
          <td>{{ stock.name }}</td>
          <td>{{ stock.pe }}</td>
          <td>{{ stock.price }}</td>
          <td>{{ stock.target_price }}</td>
          <td>{{ stock.market_cap }}</td>
          <td>{{ stock.price_prob_1 }}</td>
          <td>{{ stock.recomm_mean }}</td>
          <td>{{ stock.growth_rate_stability }}</td>
          <td>{{ stock.predict_eps_cagr }}</td>
          <td>{{ stock.ath_discount }}</td>
        </tr>
      </tbody>
    </table>
  </div>
</template>

<script>
import { defineComponent } from 'vue';

export default defineComponent({
  name: 'StockTable',
  props: {
    title: {
      type: String,
      required: true
    },
    stocks: {
      type: Array,
      required: true
    }
  }
});
</script>

<style scoped>
table {
  border-collapse: collapse;
  width: 100%;
}

th, td {
  border: 1px solid #ddd;
  padding: 8px;
  text-align: left;
}

th {
  background-color: #f2f2f2;
}
</style>
