import { Injectable } from '@nestjs/common';

import { PrimaryDataExtractor } from '../services/primary-data-extractor';
import { BaseCriterion, CriterionResult, CriterionContext, LIQUIDITY_WEIGHTS } from './base.criterion';

@Injectable()
export class ConditionCriterion extends BaseCriterion {
  public readonly name = 'condition';
  public readonly weight = LIQUIDITY_WEIGHTS.condition;

  private readonly conditionKeywords: Array<{ keywords: string[]; score: number; explanation: string }> = [
    { keywords: ['дизайнер', 'авторськ', 'авторск', 'ексклюзив', 'эксклюзив'], score: 10, explanation: 'Дизайнерський ремонт — преміум сегмент' },
    { keywords: ['євро', 'евро', 'euro'], score: 9, explanation: 'Євроремонт — відмінний стан' },
    { keywords: ['чудов'], score: 8, explanation: 'Чудовий стан' },
    { keywords: ['хорош', 'хороший', 'гарний', 'якісн', 'качествен'], score: 7, explanation: 'Хороший стан — готовий до заселення' },
    { keywords: ['житлов', 'жило', 'жилой', 'заселен', 'готов до'], score: 7, explanation: 'Житловий стан — готовий до заселення' },
    { keywords: ['задовільн'], score: 6, explanation: 'Задовільний стан' },
    { keywords: ['косметич', 'cosmetic', 'частков'], score: 5, explanation: 'Косметичний ремонт — задовільний стан' },
    {
      keywords: ['будівельник', 'строител', 'від забудовник'],
      score: 3,
      explanation: 'Після будівельників — потребує оздоблення',
    },
    { keywords: ['чорнов', 'чернов', 'під чистов', 'чистову обробк'], score: 2, explanation: 'Чорнова штукатурка — потребує оздоблення' },
    { keywords: ['без ремонт', 'без отделк'], score: 1, explanation: 'Без ремонту — потребує вкладень' },
    {
      keywords: ['потребує', 'потрібен', 'требует', 'під ремонт', 'нужен ремонт', 'потрібно', 'необхід'],
      score: 1,
      explanation: 'Потребує ремонту — низька ліквідність',
    },
    { keywords: ['аварійн', 'аварийн'], score: 0, explanation: 'Аварійний стан — найнижча ліквідність' },
  ];

  constructor(private readonly primaryDataExtractor: PrimaryDataExtractor) {
    super();
  }

  public evaluate(context: CriterionContext): CriterionResult {
    const { subject } = context;

    const conditionData = this.primaryDataExtractor.extractCondition(subject);

    if (!conditionData) {
      return this.createNullResult('Стан об\'єкта не вказано');
    }

    // Якщо extractCondition повернув score напряму (domRia, vector_crm, realtorUa, realEstateLvivUa) — використовуємо
    if (conditionData.score !== undefined) {
      return this.createResult(conditionData.score, `${conditionData.text} (${conditionData.score}/10)`);
    }

    // Keyword matching для OLX, MLS та текстових значень
    const conditionKey = conditionData.text.toLowerCase();

    for (const { keywords, score, explanation } of this.conditionKeywords) {
      if (keywords.some(kw => conditionKey.includes(kw))) {
        return this.createResult(score, `${explanation} (${conditionData.text})`);
      }
    }

    return this.createResult(5, `Стан: ${conditionData.text} (не класифіковано)`);
  }
}
