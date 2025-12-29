import { Injectable } from '@nestjs/common';

import { BaseCriterion, CriterionResult, CriterionContext, LIQUIDITY_WEIGHTS } from './base.criterion';

@Injectable()
export class PricePerMeterCriterion extends BaseCriterion {
  public readonly name = 'pricePerMeter';
  public readonly weight = LIQUIDITY_WEIGHTS.pricePerMeter;

  public evaluate(context: CriterionContext): CriterionResult {
    const { subject, fairPrice } = context;

    const subjectPrice = Number(subject.price) || 0;
    const subjectArea = Number(subject.totalArea) || 0;

    // Если нет данных для расчета - возвращаем null результат (вес 0)
    if (!fairPrice || !fairPrice.pricePerMeter?.median || fairPrice.pricePerMeter.median === 0) {
      return this.createNullResult('Немає даних про ціну за м² аналогів');
    }

    if (subjectPrice === 0 || subjectArea === 0) {
      return this.createNullResult('Немає даних про ціну або площу об\'єкта');
    }

    const subjectPricePerMeter = subjectPrice / subjectArea;
    const marketPricePerMeter = fairPrice.pricePerMeter.median;
    const ratio = subjectPricePerMeter / marketPricePerMeter;

    let score: number;
    let explanation: string;

    if (ratio <= 0.85) {
      score = 10;
      explanation = `Ціна за м² ($${Math.round(subjectPricePerMeter)}) значно нижче ринку ($${Math.round(marketPricePerMeter)})`;
    } else if (ratio <= 0.95) {
      score = 9;
      explanation = `Ціна за м² ($${Math.round(subjectPricePerMeter)}) нижче ринку ($${Math.round(marketPricePerMeter)})`;
    } else if (ratio <= 1.05) {
      score = 7;
      explanation = `Ціна за м² ($${Math.round(subjectPricePerMeter)}) на рівні ринку ($${Math.round(marketPricePerMeter)})`;
    } else if (ratio <= 1.15) {
      score = 5;
      explanation = `Ціна за м² ($${Math.round(subjectPricePerMeter)}) вище ринку ($${Math.round(marketPricePerMeter)})`;
    } else if (ratio <= 1.25) {
      score = 3;
      explanation = `Ціна за м² ($${Math.round(subjectPricePerMeter)}) значно вище ринку ($${Math.round(marketPricePerMeter)})`;
    } else {
      score = 2;
      explanation = `Ціна за м² ($${Math.round(subjectPricePerMeter)}) завищена відносно ринку ($${Math.round(marketPricePerMeter)})`;
    }

    return this.createResult(score, explanation);
  }
}
