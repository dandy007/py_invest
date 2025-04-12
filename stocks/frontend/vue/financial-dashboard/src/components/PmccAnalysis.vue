<template>
  <div class="pmcc-analysis card">
    <h3>Poor Man's Covered Call Analysis</h3>
    <div v-if="loading" class="loading">
      Loading PMCC combinations...
    </div>
    <div v-else-if="error" class="error">
      {{ error }}
    </div>
    <div v-else-if="combinations.length === 0" class="no-data">
      No profitable PMCC combinations found.
    </div>
    <div v-else class="combinations-grid">
      <div v-for="(combo, index) in combinations" 
           :key="index" 
           class="combination-card"
           :class="{ 'guaranteed-profit': combo.guaranteedProfit }">
        <div class="combo-header">
          <span class="combo-number">Combination {{ index + 1 }}</span>
          <span v-if="combo.guaranteedProfit" class="guaranteed-badge">Guaranteed Profit</span>
        </div>
        
        <div class="leg-details">
          <div class="long-leg">
            <h4>Long Call</h4>
            <p>Expiration: {{ formatDate(combo.longCall.expiration) }}</p>
            <p>Strike: {{ formatCurrency(combo.longCall.strike) }}</p>
            <p>Cost: {{ formatCurrency(combo.longCall.cost) }}</p>
          </div>
          
          <div class="short-leg">
            <h4>Short Call</h4>
            <p>Expiration: {{ formatDate(combo.shortCall.expiration) }}</p>
            <p>Strike: {{ formatCurrency(combo.shortCall.strike) }}</p>
            <p>Premium: {{ formatCurrency(combo.shortCall.premium) }}</p>
          </div>
        </div>
        
        <div class="profit-details">
          <div class="metric">
            <span class="label">Max Profit:</span>
            <span class="value profit">{{ formatCurrency(combo.maxProfit) }}</span>
          </div>
          <div class="metric">
            <span class="label">Break Even:</span>
            <span class="value">{{ formatCurrency(combo.breakEven) }}</span>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, watch } from 'vue';
import { analyzePmccCombinations } from '@/services/optionsApi';
import type { PmccCombination } from '@/services/optionsApi';

const props = defineProps<{
  tickerId: string;
}>();

const combinations = ref<PmccCombination[]>([]);
const loading = ref(false);
const error = ref<string | null>(null);

const loadPmccAnalysis = async () => {
  if (!props.tickerId) return;
  
  loading.value = true;
  error.value = null;
  combinations.value = [];
  
  try {
    combinations.value = await analyzePmccCombinations(props.tickerId);
  } catch (err: any) {
    error.value = err.message || 'Failed to load PMCC analysis';
  } finally {
    loading.value = false;
  }
};

const formatDate = (date: string) => {
  return new Date(date).toLocaleDateString();
};

const formatCurrency = (value: number) => {
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD'
  }).format(value);
};

watch(() => props.tickerId, (newTicker) => {
  if (newTicker) {
    loadPmccAnalysis();
  }
});

onMounted(() => {
  if (props.tickerId) {
    loadPmccAnalysis();
  }
});
</script>

<style scoped>
.pmcc-analysis {
  margin-top: 20px;
}

.pmcc-analysis h3 {
  margin-top: 0;
  margin-bottom: 20px;
  color: #333;
}

.loading, .error, .no-data {
  text-align: center;
  padding: 20px;
  color: #666;
}

.error {
  color: #dc3545;
}

.combinations-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
  gap: 20px;
  margin-top: 20px;
}

.combination-card {
  background: #f8f9fa;
  border-radius: 8px;
  padding: 15px;
  border: 1px solid #dee2e6;
  transition: transform 0.2s;
}

.combination-card:hover {
  transform: translateY(-2px);
}

.guaranteed-profit {
  border: 2px solid #28a745;
}

.combo-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 15px;
}

.combo-number {
  font-weight: 500;
  color: #495057;
}

.guaranteed-badge {
  background-color: #28a745;
  color: white;
  padding: 4px 8px;
  border-radius: 4px;
  font-size: 0.8em;
  font-weight: 500;
}

.leg-details {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 15px;
  margin-bottom: 15px;
}

.long-leg, .short-leg {
  padding: 10px;
  background: white;
  border-radius: 6px;
  box-shadow: 0 1px 3px rgba(0,0,0,0.1);
}

.leg-details h4 {
  margin: 0 0 10px 0;
  color: #495057;
  font-size: 1em;
}

.leg-details p {
  margin: 5px 0;
  font-size: 0.9em;
  color: #666;
}

.profit-details {
  margin-top: 15px;
  padding-top: 15px;
  border-top: 1px solid #dee2e6;
}

.metric {
  display: flex;
  justify-content: space-between;
  margin-bottom: 5px;
}

.label {
  color: #495057;
  font-weight: 500;
}

.value {
  color: #212529;
  font-weight: 500;
}

.value.profit {
  color: #28a745;
}
</style>