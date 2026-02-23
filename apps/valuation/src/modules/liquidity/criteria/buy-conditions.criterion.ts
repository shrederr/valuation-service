import { Injectable } from '@nestjs/common';

import { PrimaryDataExtractor } from '../services/primary-data-extractor';
import { BaseCriterion, CriterionResult, CriterionContext, LIQUIDITY_WEIGHTS } from './base.criterion';

/**
 * Критерий "Условия покупки" (0.04).
 * По ТЗ: base=0, єОселя+5, єВідновлення+5, ДМЖ+5, розстрочка+5, торг+2, переуступка+3, без комісії+4, S=min(10,raw)
 */
@Injectable()
export class BuyConditionsCriterion extends BaseCriterion {
  public readonly name = 'buyConditions';
  public readonly weight = LIQUIDITY_WEIGHTS.buyConditions;

  private readonly CONDITION_SCORES: Record<string, number> = {
    eOselya: 5,
    eVidnovlennya: 5,
    dmzh: 5,
    mortgage: 5,
    installment: 5,
    bargain: 2,
    assignment: 3,
    noCommission: 4,
    exchange: 1,
  };

  private readonly labels: Record<string, string> = {
    eOselya: 'єОселя',
    eVidnovlennya: 'єВідновлення',
    dmzh: 'ДМЖ',
    mortgage: 'іпотека/кредит',
    installment: 'розтерміновка',
    bargain: 'торг',
    assignment: 'переуступка',
    noCommission: 'без комісії',
    exchange: 'обмін',
  };

  constructor(private readonly primaryDataExtractor: PrimaryDataExtractor) {
    super();
  }

  public evaluate(context: CriterionContext): CriterionResult {
    const { subject } = context;

    const conditions = this.primaryDataExtractor.extractBuyConditions(subject);

    if (!conditions || conditions.length === 0) {
      return this.createNullResult('Умови купівлі невідомі');
    }

    let bonusSum = 0;
    const found: string[] = [];

    for (const cond of conditions) {
      const points = this.CONDITION_SCORES[cond];
      if (points) {
        bonusSum += points;
        found.push(this.labels[cond] || cond);
      }
    }

    const score = Math.min(10, bonusSum);
    const explanation = found.length > 0
      ? `Умови: ${found.join(', ')}`
      : 'Немає особливих умов купівлі';

    return this.createResult(score, explanation);
  }
}
