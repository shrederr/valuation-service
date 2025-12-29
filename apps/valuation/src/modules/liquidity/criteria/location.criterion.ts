import { Injectable } from '@nestjs/common';

import { BaseCriterion, CriterionResult, CriterionContext, LIQUIDITY_WEIGHTS } from './base.criterion';

@Injectable()
export class LocationCriterion extends BaseCriterion {
  public readonly name = 'location';
  public readonly weight = LIQUIDITY_WEIGHTS.location;

  public evaluate(context: CriterionContext): CriterionResult {
    const { subject } = context;

    // Если нет никаких данных о локации
    if (!subject.geoId && !subject.streetId && !subject.topzoneId && !subject.complexId) {
      return this.createNullResult('Немає даних про локацію');
    }

    let score = 5;
    const factors: string[] = [];

    if (subject.complexId) {
      score += 2;
      factors.push('ЖК');
    }

    if (subject.topzoneId) {
      score += 1;
      factors.push('популярний мікрорайон');
    }

    if (subject.streetId) {
      score += 1;
      factors.push('відома вулиця');
    }

    if (subject.lat && subject.lng) {
      score += 0.5;
    }

    score = Math.min(10, score);

    const explanation =
      factors.length > 0 ? `Локація: ${factors.join(', ')}` : 'Стандартна локація без особливих переваг';

    return this.createResult(score, explanation);
  }
}
