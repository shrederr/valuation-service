import { Injectable } from '@nestjs/common';

import { BaseCriterion, CriterionResult, CriterionContext, LIQUIDITY_WEIGHTS } from './base.criterion';

@Injectable()
export class FloorCriterion extends BaseCriterion {
  public readonly name = 'floor';
  public readonly weight = LIQUIDITY_WEIGHTS.floor;

  public evaluate(context: CriterionContext): CriterionResult {
    const { subject } = context;

    if (!subject.floor) {
      return this.createNullResult('Поверх не вказано');
    }

    const floor = subject.floor;
    const totalFloors = subject.totalFloors;

    let score: number;
    let explanation: string;

    if (floor === 1) {
      score = 6;
      explanation = '1 поверх - знижена ліквідність (шум, безпека)';
    } else if (totalFloors && floor === totalFloors) {
      score = 6;
      explanation = 'Останній поверх - ризик протікання';
    } else if (floor === 2) {
      score = 8;
      explanation = '2 поверх - хороша ліквідність';
    } else if (floor >= 3 && floor <= 5) {
      score = 9;
      explanation = `${floor} поверх - оптимальний`;
    } else if (totalFloors && floor > totalFloors * 0.8) {
      score = 7;
      explanation = 'Високий поверх - гарний вид, але залежність від ліфта';
    } else {
      score = 8;
      explanation = `${floor} поверх - середній`;
    }

    return this.createResult(score, explanation);
  }
}
