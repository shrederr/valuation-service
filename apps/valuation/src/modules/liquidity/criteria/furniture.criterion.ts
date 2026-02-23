import { Injectable } from '@nestjs/common';

import { PrimaryDataExtractor } from '../services/primary-data-extractor';
import { BaseCriterion, CriterionResult, CriterionContext, LIQUIDITY_WEIGHTS } from './base.criterion';

/**
 * Критерий "Мебель и техника" (0.07).
 * По ТЗ: Немає→0, Частково→5, Є→10
 */
@Injectable()
export class FurnitureCriterion extends BaseCriterion {
  public readonly name = 'furniture';
  public readonly weight = LIQUIDITY_WEIGHTS.furniture;

  constructor(private readonly primaryDataExtractor: PrimaryDataExtractor) {
    super();
  }

  public evaluate(context: CriterionContext): CriterionResult {
    const { subject } = context;

    const furnish = this.primaryDataExtractor.extractFurnish(subject);

    if (!furnish) {
      return this.createNullResult('Немає даних про меблі');
    }

    let score: number;
    let explanation: string;

    switch (furnish) {
      case 'yes':
        score = 10;
        explanation = 'Є меблі та техніка — підвищує ліквідність';
        break;
      case 'partial':
        score = 5;
        explanation = 'Частково мебльовано';
        break;
      case 'no':
        score = 0;
        explanation = 'Без меблів — знижує ліквідність';
        break;
    }

    return this.createResult(score, explanation);
  }
}
