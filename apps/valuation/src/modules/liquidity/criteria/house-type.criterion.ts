import { Injectable } from '@nestjs/common';

import { PrimaryDataExtractor } from '../services/primary-data-extractor';
import { BaseCriterion, CriterionResult, CriterionContext, LIQUIDITY_WEIGHTS } from './base.criterion';

@Injectable()
export class HouseTypeCriterion extends BaseCriterion {
  public readonly name = 'houseType';
  public readonly weight = LIQUIDITY_WEIGHTS.houseType;

  private readonly houseTypeKeywords: Array<{ keywords: string[]; score: number; explanation: string }> = [
    { keywords: ['моноліт', 'монолит', 'monolith', 'монолітно', 'монолитно'], score: 10, explanation: 'Моноліт — сучасний, надійний' },
    { keywords: ['цегл', 'кирпич', 'brick', 'силікат', 'силикат'], score: 8, explanation: 'Цегла — класичний, теплий' },
    { keywords: ['ракушняк', 'ракушечник'], score: 7, explanation: 'Ракушняк — між цеглою і блоком' },
    { keywords: ['каркас', 'frame'], score: 7, explanation: 'Каркасний — сучасний тип' },
    { keywords: ['блок', 'block', 'газоблок', 'пінобл', 'пеноблок', 'керамзіт', 'керамзит', 'шлакоблок'], score: 6, explanation: 'Блочний — середня якість' },
    { keywords: ['панел', 'panel', 'залізобетон', 'железобетон'], score: 4, explanation: 'Панельний — застарілий тип' },
    { keywords: ['старий фонд', 'старый фонд', 'дореволюц', 'історич', 'историч', 'стара цегл'], score: 3, explanation: 'Старий фонд — ризики стану будинку' },
    { keywords: ['хрущ', 'хрущов', 'хрущев'], score: 1, explanation: 'Хрущовка — найнижча ліквідність' },
  ];

  constructor(private readonly primaryDataExtractor: PrimaryDataExtractor) {
    super();
  }

  public evaluate(context: CriterionContext): CriterionResult {
    const { subject } = context;

    const houseTypeData = this.primaryDataExtractor.extractHouseType(subject);

    if (!houseTypeData) {
      return this.createNullResult('Тип будинку не вказано');
    }

    // Якщо extractHouseType повернув score (domRia wall_type з прямого маппінгу)
    if (houseTypeData.score !== undefined) {
      return this.createResult(houseTypeData.score, `${houseTypeData.text} (${houseTypeData.score}/10)`);
    }

    // Keyword matching для OLX, realtorUa, realEstateLvivUa, MLS
    const houseTypeKey = houseTypeData.text.toLowerCase();

    for (const { keywords, score, explanation } of this.houseTypeKeywords) {
      if (keywords.some(kw => houseTypeKey.includes(kw))) {
        return this.createResult(score, `${explanation} (${houseTypeData.text})`);
      }
    }

    return this.createResult(5, `Тип будинку: ${houseTypeData.text} (не класифіковано)`);
  }
}
