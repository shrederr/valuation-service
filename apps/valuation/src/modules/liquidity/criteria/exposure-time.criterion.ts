import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { UnifiedListing } from '@libs/database';

import { BaseCriterion, CriterionResult, CriterionContext, LIQUIDITY_WEIGHTS, ExposureStats } from './base.criterion';

/**
 * Критерій 14 за ТЗ: Середній час експозиції на ринку (W=0.09)
 *
 * x = avg_days_on_market по сегменту (район + тип + кімнати + площа)
 * "менше — краще": S = 10 × (xmax - x) / (xmax - xmin)
 *
 * Дані беруться з реальних знятих/проданих об'єктів:
 * час експозиції = deleted_at - published_at
 */
@Injectable()
export class ExposureTimeCriterion extends BaseCriterion {
  public readonly name = 'exposureTime';
  public readonly weight = LIQUIDITY_WEIGHTS.exposureTime;

  constructor(
    @InjectRepository(UnifiedListing)
    private readonly listingRepository: Repository<UnifiedListing>,
  ) {
    super();
  }

  public evaluate(context: CriterionContext): CriterionResult {
    const { exposureStats } = context;

    if (!exposureStats || exposureStats.count < 3) {
      return this.createNullResult('Недостатньо даних по знятих об\'єктах для оцінки часу експозиції');
    }

    const { subjectDays, minDays, maxDays, count } = exposureStats;

    // Якщо min == max — всі об'єкти з однаковим часом
    if (minDays >= maxDays) {
      return this.createResult(10, `Час експозиції в сегменті однаковий: ${Math.round(minDays)} днів (${count} об'єктів)`);
    }

    // За ТЗ: S = 10 × (xmax - x) / (xmax - xmin), менше — краще
    const score = 10 * (maxDays - subjectDays) / (maxDays - minDays);

    const explanation = `Оцінка ${Math.round(subjectDays)} днів (мін ${Math.round(minDays)}, макс ${Math.round(maxDays)}, медіана ${Math.round(exposureStats.medianDays)} днів, ${count} об'єктів у сегменті)`;

    return this.createResult(score, explanation);
  }

  /**
   * Розраховує статистику часу експозиції для сегменту об'єкта.
   * Сегмент: район (geo_id) + тип нерухомості + кімнати + діапазон площі (±30%)
   *
   * Використовує знятих/проданих об'єктів (deleted_at IS NOT NULL)
   * за останні 12 місяців.
   */
  public async calculateExposureForSegment(
    subject: UnifiedListing,
  ): Promise<ExposureStats | null> {
    const geoId = subject.geoId;
    const realtyType = subject.realtyType;
    const rooms = subject.rooms;
    const totalArea = subject.totalArea;

    // Крок 1: Точний сегмент (geo + тип + кімнати + площа ±30%)
    let rows = await this.querySegment(geoId, realtyType, rooms, totalArea, 0.3);
    if (rows && rows.length >= 5) {
      return this.buildResult(rows, subject);
    }

    // Крок 2: Розширюємо площу до ±50%
    rows = await this.querySegment(geoId, realtyType, rooms, totalArea, 0.5);
    if (rows && rows.length >= 5) {
      return this.buildResult(rows, subject);
    }

    // Крок 3: Без фільтру кімнат
    rows = await this.querySegment(geoId, realtyType, null, totalArea, 0.5);
    if (rows && rows.length >= 5) {
      return this.buildResult(rows, subject);
    }

    // Крок 4: Тільки geo + тип
    rows = await this.querySegment(geoId, realtyType, null, null, null);
    if (rows && rows.length >= 3) {
      return this.buildResult(rows, subject);
    }

    // Крок 5: Тільки тип (без geo)
    rows = await this.querySegment(null, realtyType, null, null, null);
    if (rows && rows.length >= 3) {
      return this.buildResult(rows, subject);
    }

    return null;
  }

  private async querySegment(
    geoId: number | null | undefined,
    realtyType: string,
    rooms: number | null | undefined,
    totalArea: number | null | undefined,
    areaRange: number | null,
  ): Promise<RawExposureData[] | null> {
    const params: any[] = [realtyType];
    let paramIdx = 2;

    let sql = `
      SELECT EXTRACT(EPOCH FROM (deleted_at - published_at)) / 86400.0 AS days
      FROM unified_listings
      WHERE deleted_at IS NOT NULL
        AND published_at IS NOT NULL
        AND deleted_at > published_at
        AND realty_type = $1
        AND deleted_at > NOW() - INTERVAL '12 months'
    `;

    if (geoId) {
      sql += ` AND geo_id = $${paramIdx}`;
      params.push(geoId);
      paramIdx++;
    }

    if (rooms != null) {
      sql += ` AND rooms = $${paramIdx}`;
      params.push(rooms);
      paramIdx++;
    }

    if (totalArea != null && areaRange != null) {
      const minArea = totalArea * (1 - areaRange);
      const maxArea = totalArea * (1 + areaRange);
      sql += ` AND total_area BETWEEN $${paramIdx} AND $${paramIdx + 1}`;
      params.push(minArea, maxArea);
      paramIdx += 2;
    }

    // Відсікаємо аномалії (менше 1 дня, більше 365 днів)
    sql += ` AND EXTRACT(EPOCH FROM (deleted_at - published_at)) / 86400.0 BETWEEN 1 AND 365`;

    const rows: { days: number }[] = await this.listingRepository.query(sql, params);

    if (!rows || rows.length === 0) return null;
    return rows.map(r => ({ days: Number(r.days) }));
  }

  private buildResult(rows: RawExposureData[], subject: UnifiedListing): ExposureStats {
    const sorted = rows.map(r => r.days).sort((a, b) => a - b);
    const count = sorted.length;

    // IQR фільтр для відсікання викидів
    const q1Idx = Math.floor(count * 0.25);
    const q3Idx = Math.floor(count * 0.75);
    const q1 = sorted[q1Idx];
    const q3 = sorted[q3Idx];
    const iqr = q3 - q1;
    const lowerBound = q1 - 1.5 * iqr;
    const upperBound = q3 + 1.5 * iqr;

    const filtered = sorted.filter(d => d >= lowerBound && d <= upperBound);
    if (filtered.length < 3) {
      // Якщо після фільтрації мало — використовуємо всі
      return this.computeStats(sorted, subject);
    }
    return this.computeStats(filtered, subject);
  }

  private computeStats(sorted: number[], subject: UnifiedListing): ExposureStats {
    const count = sorted.length;
    const minDays = sorted[0];
    const maxDays = sorted[count - 1];
    const sum = sorted.reduce((a, b) => a + b, 0);
    const avgDays = sum / count;

    const medianIdx = Math.floor(count / 2);
    const medianDays = count % 2 === 0
      ? (sorted[medianIdx - 1] + sorted[medianIdx]) / 2
      : sorted[medianIdx];

    // Оцінка часу для поточного об'єкта = медіана сегменту
    // (бо ми не знаємо реального часу — об'єкт ще на ринку)
    const subjectDays = medianDays;

    return { subjectDays, medianDays, avgDays, minDays, maxDays, count };
  }
}

interface RawExposureData {
  days: number;
}
