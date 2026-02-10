import { Injectable } from '@nestjs/common';

import { PrimaryDataExtractor } from '../services/primary-data-extractor';
import { BaseCriterion, CriterionResult, CriterionContext, LIQUIDITY_WEIGHTS } from './base.criterion';

/**
 * Критерий "Коммуникации" (0.05).
 * Данные из OLX: param key="communications", value="electricity,water,gas,heating,sewerage"
 *
 * Чек-лист по ТЗ:
 * electricity: +1, water: +1, gas: +3, heating: +2, sewerage: +1, internet: +1
 * Сумма: cap 10, normalize
 */
@Injectable()
export class CommunicationsCriterion extends BaseCriterion {
  public readonly name = 'communications';
  public readonly weight = LIQUIDITY_WEIGHTS.communications;

  private readonly COMM_SCORES: Record<string, number> = {
    electricity: 1,
    water: 1,
    gas: 3,
    heating: 2,
    sewerage: 1,
    internet: 1,
  };

  constructor(private readonly primaryDataExtractor: PrimaryDataExtractor) {
    super();
  }

  public evaluate(context: CriterionContext): CriterionResult {
    const { subject } = context;

    const communications = this.primaryDataExtractor.extractCommunications(subject);

    if (!communications) {
      return this.createNullResult('Немає даних про комунікації');
    }

    let totalPoints = 0;
    const found: string[] = [];

    for (const comm of communications) {
      // Нормализуем названия
      const normalized = this.normalizeCommName(comm);
      const points = this.COMM_SCORES[normalized];
      if (points) {
        totalPoints += points;
        found.push(normalized);
      }
    }

    // Max possible = 9 (1+1+3+2+1+1), scale to 10
    const score = Math.min(10, Math.round((totalPoints / 9) * 10));

    const explanation = found.length > 0
      ? `Комунікації: ${found.join(', ')} (${totalPoints} балів)`
      : 'Комунікації не розпізнані';

    return this.createResult(score, explanation);
  }

  private normalizeCommName(name: string): string {
    const lower = name.trim().toLowerCase();

    // Маппинг синонимов
    if (lower.includes('electr') || lower.includes('електр')) return 'electricity';
    if (lower.includes('water') || lower.includes('вод')) return 'water';
    if (lower.includes('gas') || lower.includes('газ')) return 'gas';
    if (lower.includes('heat') || lower.includes('опален')) return 'heating';
    if (lower.includes('sewer') || lower.includes('каналіз')) return 'sewerage';
    if (lower.includes('internet') || lower.includes('інтернет')) return 'internet';

    return lower;
  }
}
