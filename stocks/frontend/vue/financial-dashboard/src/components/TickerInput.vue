<template>
    <div class="ticker-input">
      <label for="ticker">Ticker:</label>
      <input
        id="ticker"
        type="text"
        :value="modelValue"
        @input="updateValue($event)"
        @keyup.enter="submitTicker"
        placeholder="Enter Ticker (e.g., NVDA)"
      />
      <button @click="submitTicker">Load</button>
    </div>
  </template>
  
  <script setup lang="ts">
  import { ref } from 'vue';
  
  // Props and Emits using defineProps/defineEmits
  const props = defineProps<{
    modelValue: string; // For v-model binding
  }>();
  
  const emit = defineEmits<{
    (e: 'update:modelValue', value: string): void; // For v-model binding
    (e: 'loadTicker', ticker: string): void; // Custom event
  }>();
  
  // Local ref to potentially hold intermediate input if needed,
  // but directly using modelValue and emitting updates is simpler for v-model
  // const internalTicker = ref(props.modelValue);
  
  const updateValue = (event: Event) => {
    const target = event.target as HTMLInputElement;
    emit('update:modelValue', target.value.toUpperCase()); // Update parent's v-model, force uppercase
  };
  
  const submitTicker = () => {
    if (props.modelValue.trim()) {
      emit('loadTicker', props.modelValue.trim());
    }
  };
  </script>
  
  <style scoped>
  .ticker-input {
    display: flex;
    align-items: center;
    gap: 10px;
    background-color: #f9f9f9;
    border-radius: 5px;
    padding: 8px;
  }
  .ticker-input label {
    font-weight: bold;
    margin: 0;
  }
  .ticker-input input {
    padding: 8px;
    border: 1px solid #ccc;
    border-radius: 4px;
    flex-grow: 1;
    height: 36px;
    box-sizing: border-box;
  }
  .ticker-input button {
    padding: 8px 15px;
    background-color: #007bff;
    color: white;
    border: none;
    border-radius: 4px;
    cursor: pointer;
    transition: background-color 0.2s;
    height: 36px;
    box-sizing: border-box;
  }
  .ticker-input button:hover {
    background-color: #0056b3;
  }
  </style>