import { Injectable } from '@nestjs/common';

import { BaseCriterion, CriterionResult, CriterionContext, LIQUIDITY_WEIGHTS } from './base.criterion';

/**
 * Критерий "Жилая площадь" — заменяет pricePerMeter.
 * По ТЗ: "больше лучше" — min-max нормализация площади в пуле аналогов.
 * Вес: 0.03
 */
@Injectable()
export class LivingAreaCriterion extends BaseCriterion {
  public readonly name = 'livingArea';
  public readonly weight = LIQUIDITY_WEIGHTS.livingArea;

  public evaluate(context: CriterionContext): CriterionResult {
    const { subject, analogs } = context;

    const subjectArea = Number(subject.totalArea) || 0;

    if (subjectArea === 0) {
      return this.createNullResult('Немає даних про площу об\'єкта');
    }

    if (!analogs || analogs.length === 0) {
      return this.createNullResult('Немає аналогів для порівняння площі');
    }

    // Собираем площади аналогов
    const areas = analogs
      .map((a) => Number(a.totalArea) || 0)
      .filter((a) => a > 0);

    if (areas.length === 0) {
      return this.createNullResult('Аналоги не мають даних про площу');
    }

    const minArea = Math.min(...areas);
    const maxArea = Math.max(...areas);

    let score: number;
    let explanation: string;

    if (maxArea === minArea) {
      // Все аналоги одинаковой площади
      score = 5;
      explanation = `Площа ${subjectArea} м² — аналоги мають однакову площу (${minArea} м²)`;
    } else {
      // Min-max нормализация: больше площадь = лучше
      const normalized = (subjectArea - minArea) / (maxArea - minArea);
      // Ограничиваем от 0 до 1 (объект может быть за пределами диапазона аналогов)
      const clamped = Math.max(0, Math.min(1, normalized));
      score = 2 + clamped * 8; // от 2 до 10

      if (clamped >= 0.7) {
        explanation = `Площа ${subjectArea} м² — більше за більшість аналогів (${minArea}-${maxArea} м²)`;
      } else if (clamped >= 0.3) {
        explanation = `Площа ${subjectArea} м² — середня серед аналогів (${minArea}-${maxArea} м²)`;
      } else {
        explanation = `Площа ${subjectArea} м² — менше за більшість аналогів (${minArea}-${maxArea} м²)`;
      }
    }

    return this.createResult(score, explanation);
  }
}
