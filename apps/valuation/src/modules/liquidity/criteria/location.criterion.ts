import { Injectable } from '@nestjs/common';

import { PrimaryDataExtractor } from '../services/primary-data-extractor';
import { BaseCriterion, CriterionResult, CriterionContext, LIQUIDITY_WEIGHTS } from './base.criterion';

/**
 * Критерий локации - оценивает географическое расположение объекта.
 * Объединяет "Вид із вікон" (0.04) и "Поруч (природа)" (0.03) = 0.07 согласно ТЗ.
 *
 * Использует comfort-теги из primaryData (OLX) для оценки вида:
 * - panoramic_windows → хороший вид
 * - balcony/loggia → возможность обзора
 * - closed_area → закрытая территория (зелёная зона)
 *
 * Fallback: бонусы за ЖК, topzone, street, координаты.
 */
@Injectable()
export class LocationCriterion extends BaseCriterion {
  public readonly name = 'location';
  public readonly weight = LIQUIDITY_WEIGHTS.location;

  constructor(private readonly primaryDataExtractor: PrimaryDataExtractor) {
    super();
  }

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
      score += 1;
      factors.push('ЖК');
    }

    // Популярный микрорайон
    if (subject.topzoneId) {
      score += 1;
      factors.push('популярний мікрорайон');
    }

    // Известная улица
    if (subject.streetId) {
      score += 0.5;
    }

    // Наличие координат
    if (subject.lat && subject.lng) {
      score += 0.5;
    }

    // Парсим comfort-теги из primaryData
    const comfort = this.primaryDataExtractor.extractComfort(subject);
    if (comfort) {
      if (comfort.includes('panoramic_windows')) {
        score += 2;
        factors.push('панорамні вікна');
      }
      if (comfort.includes('balcony') || comfort.includes('loggia')) {
        score += 0.5;
        factors.push('балкон');
      }
      if (comfort.includes('closed_area')) {
        score += 1;
        factors.push('закрита територія');
      }
    }

    score = Math.min(10, score);

    const explanation = factors.length > 0
      ? `Локація: ${factors.join(', ')}`
      : 'Стандартна локація без особливих переваг';

    return this.createResult(score, explanation);
  }
}
