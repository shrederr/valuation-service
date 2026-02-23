import { Injectable } from '@nestjs/common';

import { PrimaryDataExtractor } from '../services/primary-data-extractor';
import { BaseCriterion, CriterionResult, CriterionContext, LIQUIDITY_WEIGHTS } from './base.criterion';

/**
 * Критерій "Планування / кімнати" (0.08).
 * Комбінований підхід:
 * 1. Якщо є дані про тип планування → використовуємо score з маппінгу
 * 2. Якщо немає → rooms як proxy (1к→10, 2к→8, 3к→6, 4к→4, 5+→2)
 */
@Injectable()
export class FormatCriterion extends BaseCriterion {
  public readonly name = 'format';
  public readonly weight = LIQUIDITY_WEIGHTS.format;

  constructor(private readonly primaryDataExtractor: PrimaryDataExtractor) {
    super();
  }

  public evaluate(context: CriterionContext): CriterionResult {
    const { subject } = context;

    // Спробуємо отримати тип планування
    const layoutData = this.primaryDataExtractor.extractLayout(subject);

    if (layoutData?.score !== undefined) {
      return this.createResult(layoutData.score, `Планування: ${layoutData.text} (${layoutData.score}/10)`);
    }

    if (layoutData) {
      // Є текст планування, але без score — використовуємо як додаткову інфо
      // Fallback на rooms
    }

    // Rooms fallback
    if (!subject.rooms) {
      if (layoutData) {
        return this.createResult(5, `Планування: ${layoutData.text} (не класифіковано)`);
      }
      return this.createNullResult('Кількість кімнат не вказано');
    }

    const rooms = subject.rooms;
    let score: number;
    let explanation: string;

    switch (rooms) {
      case 1:
        score = 10;
        explanation = '1-кімнатна — найвища ліквідність';
        break;
      case 2:
        score = 8;
        explanation = '2-кімнатна — висока ліквідність';
        break;
      case 3:
        score = 6;
        explanation = '3-кімнатна — середня ліквідність';
        break;
      case 4:
        score = 4;
        explanation = '4-кімнатна — знижена ліквідність';
        break;
      default:
        if (rooms >= 5) {
          score = 2;
          explanation = `${rooms}-кімнатна — низька ліквідність`;
        } else {
          score = 5;
          explanation = 'Нестандартний формат';
        }
    }

    if (layoutData) {
      explanation += ` (${layoutData.text})`;
    }

    return this.createResult(score, explanation);
  }
}
