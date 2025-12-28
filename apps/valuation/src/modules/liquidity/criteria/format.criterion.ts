import { Injectable } from '@nestjs/common';

import { BaseCriterion, CriterionResult, CriterionContext, LIQUIDITY_WEIGHTS } from './base.criterion';

@Injectable()
export class FormatCriterion extends BaseCriterion {
  public readonly name = 'format';
  public readonly weight = LIQUIDITY_WEIGHTS.format;

  public evaluate(context: CriterionContext): CriterionResult {
    const { subject } = context;

    if (!subject.rooms) {
      return this.createNullResult('Кількість кімнат не вказано');
    }

    const rooms = subject.rooms;
    let score: number;
    let explanation: string;

    switch (rooms) {
      case 1:
        score = 9;
        explanation = '1-кімнатна - найвища ліквідність';
        break;
      case 2:
        score = 8;
        explanation = '2-кімнатна - висока ліквідність';
        break;
      case 3:
        score = 7;
        explanation = '3-кімнатна - хороша ліквідність';
        break;
      case 4:
        score = 5;
        explanation = '4-кімнатна - середня ліквідність';
        break;
      default:
        if (rooms > 4) {
          score = 4;
          explanation = `${rooms}-кімнатна - знижена ліквідність`;
        } else {
          score = 5;
          explanation = 'Нестандартний формат';
        }
    }

    return this.createResult(score, explanation);
  }
}
