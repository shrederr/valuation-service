import { Injectable } from '@nestjs/common';
import { UnifiedListing } from '@libs/database';

import { BaseCriterion, CriterionResult, CriterionContext, LIQUIDITY_WEIGHTS } from './base.criterion';

/**
 * Критерій "Площа" — min-max нормалізація площі серед аналогів.
 * По ТЗ: S = 10 * (x - xmin) / (xmax - xmin), "більше краще", 0-10.
 * Якщо xmin == xmax → S = 10.
 *
 * Для всіх платформ використовуємо totalArea (загальна площа).
 * Для ділянок (area) — landArea.
 */
@Injectable()
export class LivingAreaCriterion extends BaseCriterion {
  public readonly name = 'livingArea';
  public readonly weight = LIQUIDITY_WEIGHTS.livingArea;

  public evaluate(context: CriterionContext): CriterionResult {
    const { subject, analogs } = context;

    const subjectArea = this.getAreaValue(subject);

    if (!subjectArea || subjectArea === 0) {
      return this.createNullResult('Немає даних про площу об\'єкта');
    }

    if (!analogs || analogs.length === 0) {
      return this.createNullResult('Немає аналогів для порівняння площі');
    }

    const areas = analogs
      .map(a => this.getAreaValue(a))
      .filter((a): a is number => a !== null && a > 0);

    if (areas.length === 0) {
      return this.createNullResult('Аналоги не мають даних про площу');
    }

    const minArea = Math.min(...areas);
    const maxArea = Math.max(...areas);

    let score: number;
    let explanation: string;

    if (maxArea === minArea) {
      score = 10;
      explanation = `Площа ${subjectArea} м² — аналоги мають однакову площу (${minArea} м²)`;
    } else {
      const normalized = (subjectArea - minArea) / (maxArea - minArea);
      const clamped = Math.max(0, Math.min(1, normalized));
      score = 10 * clamped;

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

  /**
   * Для ділянок — landArea, для решти — totalArea (загальна площа).
   */
  private getAreaValue(listing: UnifiedListing): number | null {
    if (listing.realtyType === 'area') {
      return Number(listing.landArea) || null;
    }
    return Number(listing.totalArea) || null;
  }
}
