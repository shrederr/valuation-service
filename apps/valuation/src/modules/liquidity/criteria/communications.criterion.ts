import { Injectable } from '@nestjs/common';

import { PrimaryDataExtractor } from '../services/primary-data-extractor';
import { BaseCriterion, CriterionResult, CriterionContext, LIQUIDITY_WEIGHTS } from './base.criterion';

/**
 * Критерій "Комунікації" (0.06).
 * Різні шкали для квартир і домів/комерції/ділянок по ТЗ.
 *
 * Квартира: електрика+1, опалення+1, холодна вода+1, інтернет+2, гаряча вода+2, газ+3 = макс 10
 * Дім/комерція/ділянка: центральний водопровід+1.5, каналізація септик+1, інтернет+0.5,
 *   центральна каналізація+1.5, вивіз відходів+0.5, колодязь+1, свердловина+1, газ+1.5, електрика+1.5 = макс 10
 *
 * Для квартир: дефолт base=3 (електрика + опалення + холодна вода — є у всіх квартирах в Україні)
 */
@Injectable()
export class CommunicationsCriterion extends BaseCriterion {
  public readonly name = 'communications';
  public readonly weight = LIQUIDITY_WEIGHTS.communications;

  constructor(private readonly primaryDataExtractor: PrimaryDataExtractor) {
    super();
  }

  public evaluate(context: CriterionContext): CriterionResult {
    const { subject } = context;

    const communications = this.primaryDataExtractor.extractCommunications(subject);
    const realtyType = subject.realtyType;
    const isApartment = realtyType === 'apartment';

    // Якщо немає даних
    if (!communications) {
      // Для квартир — дефолт base=3 (електрика, опалення, холодна вода)
      if (isApartment) {
        return this.createResult(3, 'Квартира: базові комунікації (електрика, опалення, холодна вода)');
      }
      return this.createNullResult('Немає даних про комунікації');
    }

    let totalPoints: number;
    let found: string[];

    if (isApartment) {
      ({ totalPoints, found } = this.evaluateApartment(communications));
    } else {
      ({ totalPoints, found } = this.evaluateHouse(communications, subject));
    }

    const score = Math.min(10, totalPoints);

    const explanation = found.length > 0
      ? `Комунікації: ${found.join(', ')} (${totalPoints.toFixed(1)} балів)`
      : 'Комунікації не розпізнані';

    return this.createResult(score, explanation);
  }

  /**
   * Квартира: електрика+1, опалення+1, холодна вода+1, інтернет+2, гаряча вода+2, газ+3
   * Base=3: квартири в Україні завжди мають електрику, опалення, холодну воду
   */
  private evaluateApartment(communications: string[]): { totalPoints: number; found: string[] } {
    // Починаємо з base=3 (електрика, опалення, холодна вода — є у всіх квартирах)
    let totalPoints = 3;
    const found: string[] = ['електрика', 'опалення', 'холодна вода'];

    const additionalScores: Record<string, { points: number; label: string }> = {
      internet: { points: 2, label: 'інтернет' },
      hot_water: { points: 2, label: 'гаряча вода' },
      gas: { points: 3, label: 'газ' },
    };

    const counted = new Set<string>();

    for (const comm of communications) {
      const normalized = this.normalizeCommName(comm);

      // Базові вже враховані
      if (normalized === 'electricity' || normalized === 'heating' || normalized === 'cold_water' || normalized === 'water') {
        continue;
      }

      const entry = additionalScores[normalized];
      if (entry && !counted.has(entry.label)) {
        totalPoints += entry.points;
        found.push(entry.label);
        counted.add(entry.label);
      }
    }

    return { totalPoints, found };
  }

  /**
   * Дім/комерція/ділянка: центральний водопровід+1.5, каналізація септик+1, інтернет+0.5,
   * центральна каналізація+1.5, вивіз відходів+0.5, колодязь+1, свердловина+1, газ+1.5, електрика+1.5
   */
  private evaluateHouse(communications: string[], subject: { attributes?: Record<string, unknown> }): { totalPoints: number; found: string[] } {
    let totalPoints = 0;
    const found: string[] = [];
    const attrs = subject.attributes;

    // Електрика
    if (communications.includes('electricity')) {
      totalPoints += 1.5;
      found.push('електрика');
    }

    // Газ
    if (communications.includes('gas')) {
      totalPoints += 1.5;
      found.push('газ');
    }

    // Інтернет
    if (communications.includes('internet')) {
      totalPoints += 0.5;
      found.push('інтернет');
    }

    // Водопостачання — деталізація з Vector2 атрибутів
    if (communications.includes('water')) {
      const waterType = attrs?.water_type !== undefined ? Number(attrs.water_type) : 0;
      if (waterType === 1 || waterType === 4) {
        totalPoints += 1.5;
        found.push('центральний водопровід');
      }
      if (waterType === 2) {
        totalPoints += 1;
        found.push('колодязь');
      }
      if (waterType === 3 || waterType === 4) {
        totalPoints += 1;
        found.push('свердловина');
      }
      if (waterType === 0) {
        totalPoints += 1;
        found.push('вода');
      }
    }

    // Каналізація — деталізація з Vector2 атрибутів
    if (communications.includes('sewerage')) {
      const sewerType = attrs?.sewerage_type !== undefined ? Number(attrs.sewerage_type) : 0;
      if (sewerType === 1) {
        totalPoints += 1.5;
        found.push('центральна каналізація');
      } else if (sewerType === 2) {
        totalPoints += 1;
        found.push('каналізація септик');
      } else {
        totalPoints += 1;
        found.push('каналізація');
      }
    }

    // Опалення
    if (communications.includes('heating')) {
      totalPoints += 1;
      found.push('опалення');
    }

    return { totalPoints, found };
  }

  private normalizeCommName(name: string): string {
    const lower = name.trim().toLowerCase();

    if (lower.includes('electr') || lower.includes('електр')) return 'electricity';
    if (lower === 'hot_water' || lower.includes('гарячa') || lower.includes('горяч')) return 'hot_water';
    if (lower === 'cold_water' || lower.includes('холодн')) return 'cold_water';
    if (lower.includes('water') || lower.includes('вод')) return 'water';
    if (lower.includes('gas') || lower.includes('газ')) return 'gas';
    if (lower.includes('heat') || lower.includes('опален')) return 'heating';
    if (lower.includes('sewer') || lower.includes('каналіз')) return 'sewerage';
    if (lower.includes('internet') || lower.includes('інтернет')) return 'internet';

    return lower;
  }
}
