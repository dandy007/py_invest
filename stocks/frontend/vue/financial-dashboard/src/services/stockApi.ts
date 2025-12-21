import axios from 'axios';
import type { StockData } from '@/types/stock';

const API_BASE_URL = 'http://192.168.0.169:5000'; // Configure base URL

export async function fetchStockDataById(tickerId: string): Promise<StockData> {
  if (!tickerId) {
    throw new Error('Ticker ID cannot be empty');
  }
  try {
    const response = await axios.get<StockData>(`${API_BASE_URL}/stock/${tickerId}`);
    // Basic validation (can be more thorough)
    if (!response.data || !response.data.TICKER) {
      throw new Error('Invalid data received from API');
    }
    return response.data;
  } catch (error) {
    console.error(`Error fetching stock data for ${tickerId}:`, error);
    if (axios.isAxiosError(error)) {
      throw new Error(`API Error: ${error.response?.statusText || error.message}`);
    } else {
      throw new Error(`An unexpected error occurred: ${error}`);
    }
  }
}

export interface CurrentPriceResponse {
  ticker_id: string;
  price: number;
  timestamp: string;
}

export async function getCurrentPrice(tickerId: string): Promise<CurrentPriceResponse> {
  try {
    const response = await axios.get<CurrentPriceResponse>(`${API_BASE_URL}/stock/current_price/${tickerId}`);
    return response.data;
  } catch (error) {
    console.error(`Error getting current price for ${tickerId}:`, error);
    throw error;
  }
}

export async function resetStockData(tickerId: string): Promise<void> {
  if (!tickerId) {
    throw new Error('Ticker ID cannot be empty');
  }
  try {
    await axios.get(`${API_BASE_URL}/stock/reset/${tickerId}`);
  } catch (error) {
    console.error(`Error resetting stock data for ${tickerId}:`, error);
    if (axios.isAxiosError(error)) {
      throw new Error(`API Error: ${error.response?.statusText || error.message}`);
    } else {
      throw new Error(`An unexpected error occurred: ${error}`);
    }
  }
}