import { Injectable } from '@nestjs/common';

import { BaseCriterion, CriterionResult, CriterionContext, LIQUIDITY_WEIGHTS } from './base.criterion';

/**
 * Критерий локации - оценивает географическое расположение объекта.
 * Объединяет "Вид із вікон" (0.04) и "Поруч (природа)" (0.03) = 0.07 согласно ТЗ.
 *
 * Инфраструктура вынесена в отдельный критерий InfrastructureCriterion.
 */
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

    // ЖК обычно означает хорошую локацию с инфраструктурой
    if (subject.complexId) {
      score += 2;
      factors.push('ЖК');
    }

    // Популярный микрорайон
    if (subject.topzoneId) {
      score += 1;
      factors.push('популярний мікрорайон');
    }

    // Известная улица
    if (subject.streetId) {
      score += 1;
      factors.push('відома вулиця');
    }

    // Наличие координат позволяет точно определить локацию
    if (subject.lat && subject.lng) {
      score += 0.5;
    }

    score = Math.min(10, score);

    const explanation = factors.length > 0
      ? `Локація: ${factors.join(', ')}`
      : 'Стандартна локація без особливих переваг';

    return this.createResult(score, explanation);
  }
}
