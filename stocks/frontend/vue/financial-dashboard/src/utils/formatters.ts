/**
 * Formats a number into a human-readable string (e.g., B for billions, M for millions).
 * @param value The number to format.
 * @param decimals Number of decimal places.
 * @returns Formatted string.
 */
export function formatLargeNumber(value: number | null | undefined, decimals = 2): string {
    if (value === null || value === undefined || !isFinite(value)) return 'N/A';
  
    const absValue = Math.abs(value);
  
    if (absValue >= 1e12) {
      return (value / 1e12).toFixed(decimals) + 'T';
    } else if (absValue >= 1e9) {
      return (value / 1e9).toFixed(decimals) + 'B';
    } else if (absValue >= 1e6) {
      return (value / 1e6).toFixed(decimals) + 'M';
    } else if (absValue >= 1e3) {
      return (value / 1e3).toFixed(decimals) + 'k';
    } else {
      return value.toFixed(decimals);
    }
  }
  
  /**
   * Formats a number as a percentage string.
   * @param value The number (e.g., 0.25 for 25%).
   * @param decimals Number of decimal places.
   * @returns Formatted percentage string.
   */
  export function formatPercentage(value: number | null | undefined, decimals = 2): string {
    if (value === null || value === undefined || !isFinite(value)) return 'N/A';
    return (value * 100).toFixed(decimals) + '%';
  }
  
  /**
   * Formats a number as currency (e.g., $120.00).
   * @param value The number to format.
   * @param currencySymbol Currency symbol.
   * @param decimals Number of decimal places.
   * @returns Formatted currency string.
   */
  export function formatCurrency(value: number | null | undefined, currencySymbol = '$', decimals = 2): string {
    if (value === null || value === undefined || !isFinite(value)) return 'N/A';
     // Simple formatting, consider using Intl.NumberFormat for locale-specific formatting
    return currencySymbol + value.toFixed(decimals);
  }