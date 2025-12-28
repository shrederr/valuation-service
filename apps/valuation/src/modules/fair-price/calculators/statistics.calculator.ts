import { Injectable } from '@nestjs/common';

export interface PriceStatistics {
  median: number;
  average: number;
  min: number;
  max: number;
  q1: number;
  q3: number;
  iqr: number;
  count: number;
}

@Injectable()
export class StatisticsCalculator {
  public calculate(prices: number[]): PriceStatistics {
    if (prices.length === 0) {
      return {
        median: 0,
        average: 0,
        min: 0,
        max: 0,
        q1: 0,
        q3: 0,
        iqr: 0,
        count: 0,
      };
    }

    const sorted = [...prices].sort((a, b) => a - b);
    const count = sorted.length;

    const median = this.calculateMedian(sorted);
    const average = this.calculateAverage(sorted);
    const min = sorted[0];
    const max = sorted[count - 1];
    const q1 = this.calculatePercentile(sorted, 25);
    const q3 = this.calculatePercentile(sorted, 75);
    const iqr = q3 - q1;

    return {
      median: Math.round(median),
      average: Math.round(average),
      min: Math.round(min),
      max: Math.round(max),
      q1: Math.round(q1),
      q3: Math.round(q3),
      iqr: Math.round(iqr),
      count,
    };
  }

  private calculateMedian(sorted: number[]): number {
    const mid = Math.floor(sorted.length / 2);

    if (sorted.length % 2 === 0) {
      return (sorted[mid - 1] + sorted[mid]) / 2;
    }

    return sorted[mid];
  }

  private calculateAverage(values: number[]): number {
    if (values.length === 0) {
      return 0;
    }

    return values.reduce((sum, v) => sum + v, 0) / values.length;
  }

  private calculatePercentile(sorted: number[], percentile: number): number {
    if (sorted.length === 0) {
      return 0;
    }

    if (sorted.length === 1) {
      return sorted[0];
    }

    const index = (percentile / 100) * (sorted.length - 1);
    const lower = Math.floor(index);
    const upper = Math.ceil(index);

    if (lower === upper) {
      return sorted[lower];
    }

    const weight = index - lower;

    return sorted[lower] * (1 - weight) + sorted[upper] * weight;
  }
}
