import { Injectable } from '@nestjs/common';

import { StatisticsCalculator, PriceStatistics } from './statistics.calculator';

@Injectable()
export class OutlierFilter {
  private readonly IQR_MULTIPLIER = 1.5;

  public constructor(private readonly statisticsCalculator: StatisticsCalculator) {}

  public filterOutliers(prices: number[]): { filtered: number[]; removed: number[] } {
    if (prices.length <= 3) {
      return { filtered: prices, removed: [] };
    }

    const stats = this.statisticsCalculator.calculate(prices);
    const lowerBound = stats.q1 - this.IQR_MULTIPLIER * stats.iqr;
    const upperBound = stats.q3 + this.IQR_MULTIPLIER * stats.iqr;

    const filtered: number[] = [];
    const removed: number[] = [];

    for (const price of prices) {
      if (price >= lowerBound && price <= upperBound) {
        filtered.push(price);
      } else {
        removed.push(price);
      }
    }

    if (filtered.length < 3 && prices.length >= 3) {
      return { filtered: prices, removed: [] };
    }

    return { filtered, removed };
  }

  public getBounds(stats: PriceStatistics): { lower: number; upper: number } {
    return {
      lower: Math.round(stats.q1 - this.IQR_MULTIPLIER * stats.iqr),
      upper: Math.round(stats.q3 + this.IQR_MULTIPLIER * stats.iqr),
    };
  }
}
