import { Injectable } from '@nestjs/common';

import { BaseCriterion, CriterionResult, CriterionContext, LIQUIDITY_WEIGHTS } from './base.criterion';

@Injectable()
export class FloorCriterion extends BaseCriterion {
  public readonly name = 'floor';
  public readonly weight = LIQUIDITY_WEIGHTS.floor;

  public evaluate(context: CriterionContext): CriterionResult {
    const { subject } = context;

    // Для будинків та комерції поверх не впливає на ліквідність
    if (subject.realtyType === 'house' || subject.realtyType === 'area') {
      return this.createNullResult('Поверх не застосовується для цього типу');
    }

    if (!subject.floor) {
      return this.createNullResult('Поверх не вказано');
    }

    const floor = subject.floor;
    const totalFloors = subject.totalFloors;

    // Для комерції 1-й поверх — це норма (навіть перевага)
    if (subject.realtyType === 'commercial') {
      if (floor === 1) {
        return this.createResult(10, '1 поверх комерції — найвища ліквідність');
      }
      // Решта — стандартна оцінка
    }

    let score: number;
    let explanation: string;

    if (floor === 1) {
      score = 0;
      explanation = '1 поверх — найнижча ліквідність (шум, безпека)';
    } else if (totalFloors && floor === totalFloors) {
      score = 0;
      explanation = 'Останній поверх — ризик протікання';
    } else if (floor === 2) {
      score = 5;
      explanation = '2 поверх — середня ліквідність';
    } else if (totalFloors && floor > totalFloors * 0.8) {
      score = 8;
      explanation = 'Високий поверх — гарний вид';
    } else {
      // 3-й до передостаннього
      score = 10;
      explanation = `${floor} поверх — оптимальний`;
    }

    return this.createResult(score, explanation);
  }
}
