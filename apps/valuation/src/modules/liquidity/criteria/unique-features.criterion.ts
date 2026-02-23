import { Injectable } from '@nestjs/common';

import { PrimaryDataExtractor } from '../services/primary-data-extractor';
import { BaseCriterion, CriterionResult, CriterionContext, LIQUIDITY_WEIGHTS } from './base.criterion';

/**
 * Критерий "Уникальные преимущества" (0.06).
 * По ТЗ: x = count(features), min-max нормализация среди аналогов:
 * S = 10 * (x - xmin) / (xmax - xmin). Если xmin == xmax → S = 10.
 */
@Injectable()
export class UniqueFeaturesCriterion extends BaseCriterion {
  public readonly name = 'uniqueFeatures';
  public readonly weight = LIQUIDITY_WEIGHTS.uniqueFeatures;

  constructor(private readonly primaryDataExtractor: PrimaryDataExtractor) {
    super();
  }

  public evaluate(context: CriterionContext): CriterionResult {
    const { subject, analogs } = context;

    const subjectFeatures = this.primaryDataExtractor.extractComfort(subject);
    const subjectCount = subjectFeatures ? subjectFeatures.length : 0;

    if (subjectCount === 0 && (!analogs || analogs.length === 0)) {
      return this.createNullResult('Немає даних про комфорт та переваги');
    }

    // Min-max нормализация среди аналогов
    if (analogs && analogs.length > 0) {
      const analogCounts = analogs.map((a) => {
        const features = this.primaryDataExtractor.extractComfort(a);
        return features ? features.length : 0;
      });

      const allCounts = [...analogCounts, subjectCount];
      const xmin = Math.min(...allCounts);
      const xmax = Math.max(...allCounts);

      let score: number;
      if (xmax === xmin) {
        score = 10;
      } else {
        const normalized = (subjectCount - xmin) / (xmax - xmin);
        const clamped = Math.max(0, Math.min(1, normalized));
        score = 10 * clamped;
      }

      const explanation = subjectFeatures
        ? `Переваги (${subjectCount}): ${subjectFeatures.join(', ')}`
        : `Кількість переваг: ${subjectCount}`;

      return this.createResult(score, explanation);
    }

    // Fallback: без аналогов — оценка только по количеству
    if (subjectCount === 0) {
      return this.createResult(0, 'Немає додаткових переваг');
    }

    // Простая шкала: каждая фича = ~1.5 балла, cap 10
    const score = Math.min(10, subjectCount * 1.5);
    const explanation = subjectFeatures
      ? `Переваги (${subjectCount}): ${subjectFeatures.join(', ')}`
      : `Кількість переваг: ${subjectCount}`;

    return this.createResult(score, explanation);
  }
}
