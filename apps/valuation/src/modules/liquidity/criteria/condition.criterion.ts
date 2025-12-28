import { Injectable } from '@nestjs/common';

import { BaseCriterion, CriterionResult, CriterionContext, LIQUIDITY_WEIGHTS } from './base.criterion';

@Injectable()
export class ConditionCriterion extends BaseCriterion {
  public readonly name = 'condition';
  public readonly weight = LIQUIDITY_WEIGHTS.condition;

  private readonly conditionKeywords: Array<{ keywords: string[]; score: number; explanation: string }> = [
    { keywords: ['дизайнер', 'авторськ', 'авторск', 'ексклюзив', 'эксклюзив'], score: 9, explanation: 'Дизайнерський ремонт - преміум сегмент' },
    { keywords: ['євро', 'евро', 'euro'], score: 8, explanation: 'Євроремонт - хороший стан' },
    { keywords: ['хорош', 'хороший', 'гарний', 'якісн', 'качествен'], score: 7, explanation: 'Хороший стан - готовий до заселення' },
    { keywords: ['житлов', 'жило', 'жилой', 'заселен', 'готов до'], score: 7, explanation: 'Житловий стан - готовий до заселення' },
    { keywords: ['косметич', 'cosmetic'], score: 6, explanation: 'Косметичний ремонт - задовільний стан' },
    { keywords: ['будівельник', 'строител', 'чорнов', 'чернов', 'від забудовник'], score: 5, explanation: 'Після будівельників - потребує оздоблення' },
    { keywords: ['без ремонт', 'без отделк'], score: 4, explanation: 'Без ремонту - потребує вкладень' },
    { keywords: ['потребує', 'потрібен', 'требует', 'під ремонт', 'нужен ремонт', 'потрібно', 'необхід'], score: 3, explanation: 'Потребує ремонту - знижена ліквідність' },
  ];

  public evaluate(context: CriterionContext): CriterionResult {
    const { subject } = context;

    if (!subject.condition) {
      return this.createNullResult('Стан об\'єкта не вказано');
    }

    const conditionKey = subject.condition.toLowerCase();

    for (const { keywords, score, explanation } of this.conditionKeywords) {
      if (keywords.some(kw => conditionKey.includes(kw))) {
        return this.createResult(score, explanation);
      }
    }

    return this.createResult(6, `Стан: ${subject.condition} (не класифіковано)`);
  }
}
