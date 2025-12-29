import { Injectable } from '@nestjs/common';

import { BaseCriterion, CriterionResult, CriterionContext, LIQUIDITY_WEIGHTS } from './base.criterion';

@Injectable()
export class HouseTypeCriterion extends BaseCriterion {
  public readonly name = 'houseType';
  public readonly weight = LIQUIDITY_WEIGHTS.houseType;

  private readonly houseTypeKeywords: Array<{ keywords: string[]; score: number; explanation: string }> = [
    { keywords: ['моноліт', 'монолит', 'monolith', 'монолітно', 'монолитно'], score: 9, explanation: 'Моноліт - сучасний, надійний' },
    { keywords: ['цегл', 'кирпич', 'brick', 'силікат', 'силикат'], score: 8, explanation: 'Цегла - класичний, теплий' },
    { keywords: ['каркас', 'frame'], score: 7, explanation: 'Каркасний - сучасний тип' },
    { keywords: ['блок', 'block', 'газоблок', 'пінобл', 'пеноблок'], score: 6, explanation: 'Блочний - середня якість' },
    { keywords: ['панел', 'panel'], score: 5, explanation: 'Панельний - застарілий тип' },
    { keywords: ['старий фонд', 'старый фонд', 'дореволюц', 'історич', 'историч'], score: 4, explanation: 'Старий фонд - ризики стану будинку' },
    { keywords: ['хрущ', 'хрущов', 'хрущев'], score: 4, explanation: 'Хрущовка - застарілий тип' },
  ];

  public evaluate(context: CriterionContext): CriterionResult {
    const { subject } = context;

    if (!subject.houseType) {
      return this.createNullResult('Тип будинку не вказано');
    }

    const houseTypeKey = subject.houseType.toLowerCase();

    for (const { keywords, score, explanation } of this.houseTypeKeywords) {
      if (keywords.some(kw => houseTypeKey.includes(kw))) {
        return this.createResult(score, explanation);
      }
    }

    return this.createResult(6, `Тип будинку: ${subject.houseType} (не класифіковано)`);
  }
}
