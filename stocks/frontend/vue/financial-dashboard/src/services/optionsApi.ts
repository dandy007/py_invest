import axios from 'axios';

const BASE_URL = 'http://192.168.0.169:5000';

interface GrowthProbabilityResponse {
  probabilities: { [key: string]: number };
  statistics: {
    total_prices: number;
    periods_analyzed: number;
    mean_change?: number;
    std_dev?: number;
    std_dev_2?: number;
    min_change?: number;
    max_change?: number;
  };
  histogram: Array<{
    bin_start: number;
    bin_end: number;
    count: number;
    markers: string[];
  }>;
}

export interface OptionChainItem {
  strike: number;
  last_price: number;
  bid: number;
  ask: number;
  volume: number;
  open_interest: number;
}

interface OptionChainResponse {
  expiration: string;
  option_type: string;
  option_chain: OptionChainItem[];
}

interface ExpirationDatesResponse {
  ticker_id: string;
  expiration_dates: string[];
}

export type ImpliedVolatilityResponse = [string[], number[]]; // [dates, iv_values]

export const getGrowthProbability = async (
  tickerId: string,
  days: number = 30,
  percentRange: number = 20
): Promise<GrowthProbabilityResponse> => {
  try {
    const response = await axios.get<GrowthProbabilityResponse>(
      `${BASE_URL}/options/growth_probability/${tickerId}/${days}/${percentRange}`
    );
    return response.data;
  } catch (error) {
    console.error('Error fetching growth probability:', error);
    throw error;
  }
};

export const getGrowthProbabilityMonteCarlo = async (
  tickerId: string,
  days: number = 30,
  percentRange: number = 20
): Promise<GrowthProbabilityResponse> => {
  try {
    const response = await axios.get<GrowthProbabilityResponse>(
      `${BASE_URL}/options/growth_probability_mc/${tickerId}/${days}/${percentRange}`
    );
    return response.data;
  } catch (error) {
    console.error('Error fetching Monte Carlo growth probability:', error);
    throw error;
  }
};

export const getOptionChain = async (
  tickerId: string,
  expiration: string,
  optionType: 'call' | 'put'
): Promise<OptionChainResponse> => {
  try {
    const response = await axios.get<OptionChainResponse>(
      `${BASE_URL}/options/get_chain/${tickerId}/${expiration}/${optionType}`
    );
    return response.data;
  } catch (error) {
    console.error('Error fetching option chain:', error);
    throw error;
  }
};

export const getExpirationDates = async (tickerId: string): Promise<string[]> => {
  try {
    const response = await axios.get<ExpirationDatesResponse>(
      `${BASE_URL}/options/get_expirations/${tickerId}`
    );
    return response.data.expiration_dates;
  } catch (error) {
    console.error('Error fetching expiration dates:', error);
    throw error;
  }
};

export const getImpliedVolatility = async (
  tickerId: string,
  expiration: string,
  strike: number
): Promise<[string[], number[]]> => {
  try {
    const response = await axios.get<ImpliedVolatilityResponse>(
      `${BASE_URL}/options/get_iv/${tickerId}/${expiration}/${strike}`
    );
    return response.data;
  } catch (error) {
    console.error('Error fetching implied volatility:', error);
    throw error;
  }
};