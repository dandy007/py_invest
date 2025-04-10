import { defineStore } from 'pinia';

interface HistogramSettings {
  days: number;
  percentRange: number;
}

export const useTickerStore = defineStore('ticker', {
  state: () => ({
    currentTicker: 'NVDA',
    histogramSettings: {
      days: 30,
      percentRange: 20
    }
  }),
  actions: {
    setTicker(ticker: string) {
      this.currentTicker = ticker;
    },
    updateHistogramSettings(settings: Partial<HistogramSettings>) {
      this.histogramSettings = {
        ...this.histogramSettings,
        ...settings
      };
    }
  }
});