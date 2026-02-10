import { Injectable } from '@nestjs/common';

import { PrimaryDataExtractor } from '../services/primary-data-extractor';
import { BaseCriterion, CriterionResult, CriterionContext, LIQUIDITY_WEIGHTS } from './base.criterion';

/**
 * Критерий "Уникальные преимущества + Комфорт" (0.06).
 * Данные из OLX: param key="comfort", value="elevator,intercom,parking,balcony,closed_area,panoramic_windows..."
 *
 * Бонусы:
 * elevator: +1, parking: +1.5, balcony/loggia: +1, closed_area: +1.5,
 * intercom/video_intercom: +0.5, panoramic_windows: +1, conditioner: +0.5
 * other: +0.5 each (cap 2)
 * Score = min(10, 3 + sum_of_bonuses)
 */
@Injectable()
export class UniqueFeaturesCriterion extends BaseCriterion {
  public readonly name = 'uniqueFeatures';
  public readonly weight = LIQUIDITY_WEIGHTS.uniqueFeatures;

  private readonly FEATURE_SCORES: Record<string, number> = {
    elevator: 1,
    parking: 1.5,
    balcony: 1,
    loggia: 1,
    closed_area: 1.5,
    intercom: 0.5,
    video_intercom: 0.5,
    panoramic_windows: 1,
    conditioner: 0.5,
  };

  constructor(private readonly primaryDataExtractor: PrimaryDataExtractor) {
    super();
  }

  public evaluate(context: CriterionContext): CriterionResult {
    const { subject } = context;

    const comfort = this.primaryDataExtractor.extractComfort(subject);

    if (!comfort) {
      return this.createNullResult('Немає даних про комфорт та переваги');
    }

    let bonusSum = 0;
    let otherBonus = 0;
    const features: string[] = [];

    for (const item of comfort) {
      const known = this.FEATURE_SCORES[item];
      if (known !== undefined) {
        bonusSum += known;
        features.push(item);
      } else {
        otherBonus += 0.5;
      }
    }

    // Cap other bonuses at 2
    otherBonus = Math.min(2, otherBonus);
    bonusSum += otherBonus;

    const score = Math.min(10, 3 + bonusSum);

    const explanation = features.length > 0
      ? `Переваги: ${features.join(', ')}`
      : 'Є додаткові переваги';

    return this.createResult(score, explanation);
  }
}
