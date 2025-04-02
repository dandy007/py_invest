/**
 * Calculates Compound Annual Growth Rate (CAGR).
 * @param values Array of numerical values.
 * @param years Number of years the values span.
 * @returns CAGR percentage or null if calculation is not possible.
 */
export function calculateCAGR(values: (number | null)[], years: number): number | null {
    if (!values || values.length < 2 || years <= 0) {
      return null;
    }
  
    const startValue = values[0];
    const endValue = values[values.length - 1];
  
    if (startValue === null || endValue === null || startValue === 0) {
        // Handle cases where start/end is null or start is zero
        // Depending on requirements, might return 0 or null
        // Let's return null for clearer indication of non-calculation
        return null;
    }
  
    // Handle negative start value carefully - CAGR might not be meaningful
    if (startValue < 0 && endValue > 0) {
        return null; // Or handle as per specific financial interpretation
    }
     if (startValue < 0 && endValue < 0) {
        // Both negative, calculate based on absolute values? Or indicate invalid?
        // For simplicity, let's proceed but be aware of interpretation limits
        const cagr = (Math.pow(Math.abs(endValue / startValue), 1 / years) - 1);
        return cagr * 100;
     }
      if (startValue > 0 && endValue < 0) {
        return null; // Growth from positive to negative often doesn't use CAGR
    }
  
  
    const cagr = (Math.pow(endValue / startValue, 1 / years)) - 1;
  
    // Check for NaN or Infinity in case of invalid inputs leading to weird math
    if (!isFinite(cagr)) {
      return null;
    }
  
    return cagr * 100;
  }
  
  /**
   * Estimates the number of years between the first and last date in a list.
   * Assumes dates are sorted chronologically.
   * @param dates Array of date strings (YYYY-MM-DD or similar ISO format).
   * @returns Number of years or 0 if insufficient data.
   */
  export function estimateYears(dates: string[]): number {
      if (!dates || dates.length < 2) {
          return 0;
      }
      try {
          const startDate = new Date(dates[0]);
          const endDate = new Date(dates[dates.length - 1]);
          const diffTime = Math.abs(endDate.getTime() - startDate.getTime());
          const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
          return diffDays / 365.25; // Approximate years using average days
      } catch (e) {
          console.error("Error parsing dates for year estimation:", e);
          return 0;
      }
  }