import { Injectable } from '@nestjs/common';

import { PrimaryDataExtractor } from '../services/primary-data-extractor';
import { BaseCriterion, CriterionResult, CriterionContext, LIQUIDITY_WEIGHTS } from './base.criterion';

@Injectable()
export class HouseTypeCriterion extends BaseCriterion {
  public readonly name = 'houseType';
  public readonly weight = LIQUIDITY_WEIGHTS.houseType;

  private readonly houseTypeKeywords: Array<{ keywords: string[]; score: number; explanation: string }> = [
    // Сучасні (10)
    { keywords: ['новобуд', 'новострой', 'new build'], score: 10, explanation: 'Новобуд — сучасний, найвища ліквідність' },
    { keywords: ['моноліт', 'монолит', 'monolith', 'монолітно', 'монолитно'], score: 10, explanation: 'Моноліт — сучасний, надійний' },
    // Сучасні формати (8-9)
    { keywords: ['таунхаус', 'таунхауз', 'townhouse'], score: 9, explanation: 'Таунхаус — сучасний формат' },
    { keywords: ['дуплекс', 'duplex'], score: 8, explanation: 'Дуплекс — сучасний формат' },
    // Якісні (7-8)
    { keywords: ['цегл', 'кирпич', 'brick', 'силікат', 'силикат'], score: 8, explanation: 'Цегла — класичний, теплий' },
    { keywords: ['сталін', 'сталин'], score: 7, explanation: 'Сталінка — якісний радянський фонд' },
    { keywords: ['чеськ', 'чешк', 'чеська'], score: 7, explanation: 'Чеська — якісний радянський проект' },
    { keywords: ['австрій', 'австрий'], score: 7, explanation: 'Австрійська — якісний проект' },
    { keywords: ['ракушняк', 'ракушечник'], score: 7, explanation: 'Ракушняк — між цеглою і блоком' },
    { keywords: ['газобетон', 'газоблок'], score: 7, explanation: 'Газобетон — сучасний матеріал' },
    // Середні (5-6)
    { keywords: ['спецпроект'], score: 6, explanation: 'Спецпроект — стандартний тип' },
    { keywords: ['болгарськ', 'болгарск'], score: 6, explanation: 'Болгарська — середній радянський проект' },
    { keywords: ['московськ', 'московск'], score: 6, explanation: 'Московський — середній радянський проект' },
    { keywords: ['югослав'], score: 6, explanation: 'Югославський — середній проект' },
    { keywords: ['блок', 'block', 'пінобл', 'пеноблок', 'керамзіт', 'керамзит', 'шлакоблок', 'термоблок'], score: 6, explanation: 'Блочний — середня якість' },
    { keywords: ['каркас', 'frame'], score: 6, explanation: 'Каркасний тип' },
    { keywords: ['будинок', 'будинку'], score: 5, explanation: 'Будинок — стандартний тип' },
    { keywords: ['дерев', 'wood'], score: 5, explanation: 'Дерево — залежить від стану' },
    { keywords: ['дача'], score: 5, explanation: 'Дача — сезонний формат' },
    { keywords: ['бетон', 'залізобетон', 'железобетон', 'метал'], score: 5, explanation: 'Бетон / залізобетон' },
    { keywords: ['київка', 'киевка'], score: 6, explanation: 'Київка — середній проект' },
    { keywords: ['польськ', 'польск'], score: 6, explanation: 'Польська — середній проект' },
    { keywords: ['бельгій', 'бельгий'], score: 6, explanation: 'Бельгійська — середній проект' },
    // Низькі (3-4)
    { keywords: ['панел', 'panel'], score: 4, explanation: 'Панельний — застарілий тип' },
    { keywords: ['старий фонд', 'старый фонд', 'дореволюц', 'історич', 'историч', 'стара цегл', 'царськ', 'царск'], score: 3, explanation: 'Старий фонд — ризики стану будинку' },
    { keywords: ['частина будинку', 'часть дома'], score: 3, explanation: 'Частина будинку — обмежена ліквідність' },
    { keywords: ['гостин'], score: 3, explanation: 'Гостинка — малогабаритна, низький попит' },
    { keywords: ['малосімей', 'малосемей'], score: 3, explanation: 'Малосімейка — малогабаритна' },
    { keywords: ['сотов'], score: 3, explanation: 'Сотовий — застарілий проект' },
    // Найнижча (1)
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
