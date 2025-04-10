import axios from 'axios';

const BASE_URL = 'http://localhost:5000';

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