import { Injectable } from '@nestjs/common';

import { BaseCriterion, CriterionResult, CriterionContext, LIQUIDITY_WEIGHTS } from './base.criterion';

@Injectable()
export class CompetitionCriterion extends BaseCriterion {
  public readonly name = 'competition';
  public readonly weight = LIQUIDITY_WEIGHTS.competition;

  public evaluate(context: CriterionContext): CriterionResult {
    const { fairPrice } = context;

    if (!fairPrice || fairPrice.analogsCount === undefined) {
      return this.createNullResult('Немає даних про кількість аналогів');
    }

    const analogsCount = fairPrice.analogsCount;

    // Линейная интерполяция: ≤5 → 10, ≥50 → 0
    let score: number;
    let explanation: string;

    if (analogsCount <= 5) {
      score = 10;
      explanation = `Низька конкуренція (${analogsCount} аналогів) — легше продати`;
    } else if (analogsCount >= 50) {
      score = 0;
      explanation = `Перенасичений ринок (${analogsCount} аналогів) — потрібна конкурентна ціна`;
    } else {
      // Линейная интерполяция между 5 и 50
      score = 10 * (50 - analogsCount) / (50 - 5);
      if (analogsCount <= 15) {
        explanation = `Помірна конкуренція (${analogsCount} аналогів)`;
      } else if (analogsCount <= 30) {
        explanation = `Висока конкуренція (${analogsCount} аналогів)`;
      } else {
        explanation = `Дуже висока конкуренція (${analogsCount} аналогів)`;
      }
    }

    return this.createResult(score, explanation);
  }
}
