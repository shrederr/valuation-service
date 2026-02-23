import { Injectable } from '@nestjs/common';

import { BaseCriterion, CriterionResult, CriterionContext, LIQUIDITY_WEIGHTS } from './base.criterion';

@Injectable()
export class PriceCriterion extends BaseCriterion {
  public readonly name = 'price';
  public readonly weight = LIQUIDITY_WEIGHTS.price;

  public evaluate(context: CriterionContext): CriterionResult {
    const { subject, analogs, fairPrice } = context;

    if (!subject.price) {
      return this.createNullResult('Немає даних про ціну об\'єкта');
    }

    const subjectPrice = Number(subject.price);

    // Min-max нормализация среди аналогов (ТЗ: S = 10 * (xmax - x) / (xmax - xmin))
    if (analogs && analogs.length > 0) {
      const prices = analogs
        .map((a) => Number(a.price))
        .filter((p) => p > 0);

      if (prices.length > 0) {
        const xmin = Math.min(...prices);
        const xmax = Math.max(...prices);

        if (xmax === xmin) {
          // Все аналоги одной цены
          const score = subjectPrice <= xmin ? 10 : 0;
          return this.createResult(score, `Ціна ${subjectPrice} — аналоги мають однакову ціну (${xmin})`);
        }

        // "Меньше лучше": S = 10 * (xmax - x) / (xmax - xmin)
        const normalized = (xmax - subjectPrice) / (xmax - xmin);
        const clamped = Math.max(0, Math.min(1, normalized));
        const score = 10 * clamped;

        let explanation: string;
        if (clamped >= 0.7) {
          explanation = `Ціна ${subjectPrice} — нижче більшості аналогів (${xmin}-${xmax})`;
        } else if (clamped >= 0.3) {
          explanation = `Ціна ${subjectPrice} — середня серед аналогів (${xmin}-${xmax})`;
        } else {
          explanation = `Ціна ${subjectPrice} — вище більшості аналогів (${xmin}-${xmax})`;
        }

        return this.createResult(score, explanation);
      }
    }

    // Fallback: ratio-based шкала 0-10
    if (!fairPrice || fairPrice.median === 0) {
      return this.createNullResult('Немає даних про ринкову ціну аналогів');
    }

    const ratio = subjectPrice / fairPrice.median;

    let score: number;
    let explanation: string;

    if (ratio <= 0.85) {
      score = 10;
      explanation = 'Ціна значно нижче ринкової — висока ліквідність';
    } else if (ratio <= 0.95) {
      score = 8;
      explanation = 'Ціна трохи нижче ринкової — добра ліквідність';
    } else if (ratio <= 1.05) {
      score = 6;
      explanation = 'Ціна на рівні ринкової';
    } else if (ratio <= 1.15) {
      score = 4;
      explanation = 'Ціна вище ринкової на 5-15%';
    } else if (ratio <= 1.3) {
      score = 2;
      explanation = 'Ціна значно вище ринкової';
    } else {
      score = 0;
      explanation = 'Ціна завищена — низька ліквідність';
    }

    return this.createResult(score, explanation);
  }
}
