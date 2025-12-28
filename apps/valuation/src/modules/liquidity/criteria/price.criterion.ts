import { Injectable } from '@nestjs/common';

import { BaseCriterion, CriterionResult, CriterionContext, LIQUIDITY_WEIGHTS } from './base.criterion';

@Injectable()
export class PriceCriterion extends BaseCriterion {
  public readonly name = 'price';
  public readonly weight = LIQUIDITY_WEIGHTS.price;

  public evaluate(context: CriterionContext): CriterionResult {
    const { subject, fairPrice } = context;

    if (!fairPrice || !subject.price || fairPrice.median === 0) {
      return this.createNullResult('Немає даних про ринкову ціну аналогів');
    }

    const subjectPrice = Number(subject.price);
    const ratio = subjectPrice / fairPrice.median;

    let score: number;
    let explanation: string;

    if (ratio <= 0.85) {
      score = 10;
      explanation = 'Ціна значно нижче ринкової - висока ліквідність';
    } else if (ratio <= 0.95) {
      score = 9;
      explanation = 'Ціна трохи нижче ринкової - добра ліквідність';
    } else if (ratio <= 1.0) {
      score = 8;
      explanation = 'Ціна на рівні ринкової - нормальна ліквідність';
    } else if (ratio <= 1.05) {
      score = 7;
      explanation = 'Ціна трохи вище ринкової';
    } else if (ratio <= 1.1) {
      score = 6;
      explanation = 'Ціна вище ринкової на 5-10%';
    } else if (ratio <= 1.15) {
      score = 5;
      explanation = 'Ціна вище ринкової на 10-15%';
    } else if (ratio <= 1.2) {
      score = 4;
      explanation = 'Ціна вище ринкової на 15-20%';
    } else if (ratio <= 1.3) {
      score = 3;
      explanation = 'Ціна значно вище ринкової';
    } else {
      score = 2;
      explanation = 'Ціна завищена - низька ліквідність';
    }

    return this.createResult(score, explanation);
  }
}
