import { Injectable } from '@nestjs/common';

import { BaseCriterion, CriterionResult, CriterionContext, LIQUIDITY_WEIGHTS } from './base.criterion';

@Injectable()
export class CompetitionCriterion extends BaseCriterion {
  public readonly name = 'competition';
  public readonly weight = LIQUIDITY_WEIGHTS.competition;

  public evaluate(context: CriterionContext): CriterionResult {
    const { fairPrice } = context;

    if (!fairPrice || fairPrice.analogsCount === undefined) {
      return this.createNullResult('Немає даних про кількість аналогів');
    }

    const analogsCount = fairPrice.analogsCount;

    let score: number;
    let explanation: string;

    if (analogsCount <= 5) {
      score = 10;
      explanation = `Низька конкуренція (${analogsCount} аналогів) - легше продати`;
    } else if (analogsCount <= 10) {
      score = 8;
      explanation = `Помірна конкуренція (${analogsCount} аналогів)`;
    } else if (analogsCount <= 15) {
      score = 7;
      explanation = `Середня конкуренція (${analogsCount} аналогів)`;
    } else if (analogsCount <= 20) {
      score = 6;
      explanation = `Підвищена конкуренція (${analogsCount} аналогів)`;
    } else if (analogsCount <= 30) {
      score = 5;
      explanation = `Висока конкуренція (${analogsCount} аналогів) - складніше виділитися`;
    } else if (analogsCount <= 50) {
      score = 4;
      explanation = `Дуже висока конкуренція (${analogsCount} аналогів)`;
    } else {
      score = 3;
      explanation = `Перенасичений ринок (${analogsCount} аналогів) - потрібна конкурентна ціна`;
    }

    return this.createResult(score, explanation);
  }
}
