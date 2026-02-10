import { Injectable } from '@nestjs/common';

import { PrimaryDataExtractor } from '../services/primary-data-extractor';
import { BaseCriterion, CriterionResult, CriterionContext, LIQUIDITY_WEIGHTS } from './base.criterion';

/**
 * Критерий "Условия покупки" (0.04).
 * Парсит из description: єОселя, іпотека, розтерміновка, рассрочка, торг, обмін.
 *
 * Бонусы:
 * єОселя/іпотека: +3, розтерміновка: +3, торг: +2, обмін: +1
 * base: 4
 * Score = min(10, base + bonuses)
 */
@Injectable()
export class BuyConditionsCriterion extends BaseCriterion {
  public readonly name = 'buyConditions';
  public readonly weight = LIQUIDITY_WEIGHTS.buyConditions;

  private readonly CONDITION_SCORES: Record<string, number> = {
    eOselya: 3,
    mortgage: 3,
    installment: 3,
    bargain: 2,
    exchange: 1,
  };

  constructor(private readonly primaryDataExtractor: PrimaryDataExtractor) {
    super();
  }

  public evaluate(context: CriterionContext): CriterionResult {
    const { subject } = context;

    const conditions = this.primaryDataExtractor.extractBuyConditions(subject);

    if (!conditions) {
      return this.createNullResult('Немає даних про умови купівлі');
    }

    let bonusSum = 0;
    const found: string[] = [];
    const labels: Record<string, string> = {
      eOselya: 'єОселя',
      mortgage: 'іпотека',
      installment: 'розтерміновка',
      bargain: 'торг',
      exchange: 'обмін',
    };

    for (const cond of conditions) {
      const points = this.CONDITION_SCORES[cond];
      if (points) {
        bonusSum += points;
        found.push(labels[cond] || cond);
      }
    }

    const score = Math.min(10, 4 + bonusSum);
    const explanation = `Умови: ${found.join(', ')}`;

    return this.createResult(score, explanation);
  }
}
