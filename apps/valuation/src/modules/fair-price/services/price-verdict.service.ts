import { Injectable } from '@nestjs/common';

import { PriceStatistics } from '../calculators/statistics.calculator';

export type PriceVerdict = 'cheap' | 'in_market' | 'expensive';

@Injectable()
export class PriceVerdictService {
  private readonly CHEAP_THRESHOLD = 0.9;
  private readonly EXPENSIVE_THRESHOLD = 1.1;

  public determineVerdict(subjectPrice: number, stats: PriceStatistics): PriceVerdict {
    if (stats.median === 0) {
      return 'in_market';
    }

    const ratio = subjectPrice / stats.median;

    if (ratio < this.CHEAP_THRESHOLD) {
      return 'cheap';
    }

    if (ratio > this.EXPENSIVE_THRESHOLD) {
      return 'expensive';
    }

    return 'in_market';
  }

  public getVerdictExplanation(verdict: PriceVerdict, subjectPrice: number, stats: PriceStatistics): string {
    const percentDiff = Math.abs((subjectPrice - stats.median) / stats.median) * 100;

    switch (verdict) {
      case 'cheap':
        return `Ціна на ${percentDiff.toFixed(1)}% нижче медіанної ринкової ціни`;
      case 'expensive':
        return `Ціна на ${percentDiff.toFixed(1)}% вище медіанної ринкової ціни`;
      case 'in_market':
        return `Ціна відповідає ринковій (±${percentDiff.toFixed(1)}% від медіани)`;
    }
  }
}
