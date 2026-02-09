import { Injectable } from '@nestjs/common';

import { BaseCriterion, CriterionResult, CriterionContext } from './base.criterion';

/**
 * Критерий "Поруч (інфраструктура)" согласно ТЗ (Оценка ликвидности.xlsx, row 38)
 * Вес: 0.07
 *
 * Высокий приоритет (из OSM данных):
 * - школа, дитсадок (nearestSchool)
 * - больница, аптека (nearestHospital)
 * - супермаркет, рынок (nearestSupermarket)
 * - транспортная остановка (nearestPublicTransport)
 *
 * Низкий приоритет:
 * - парковка (nearestParking)
 *
 * Логика: "Нормализация данных, у кого больше преимуществ, тот лучше"
 * Близость к инфраструктуре = более высокий балл
 */
@Injectable()
export class InfrastructureCriterion extends BaseCriterion {
  public readonly name = 'infrastructure';
  public readonly weight = 0.07;

  // Пороговые расстояния в метрах
  private readonly DISTANCE_THRESHOLDS = {
    excellent: 300,   // Отлично - до 300м (5 минут пешком)
    good: 500,        // Хорошо - до 500м
    average: 800,     // Средне - до 800м (10 минут пешком)
    belowAverage: 1200, // Ниже среднего - до 1200м
    // > 1200м - далеко
  };

  public evaluate(context: CriterionContext): CriterionResult {
    const { subject } = context;

    // Проверяем наличие данных об инфраструктуре
    const hasInfraData = subject.nearestSchool != null ||
                         subject.nearestHospital != null ||
                         subject.nearestSupermarket != null ||
                         subject.nearestPublicTransport != null ||
                         subject.nearestParking != null;

    if (!hasInfraData) {
      return this.createNullResult('Немає даних про інфраструктуру');
    }

    let totalScore = 0;
    let count = 0;
    const factors: string[] = [];

    // Высокий приоритет (больший вес в расчете)

    // 1. Транспорт (самый важный) - вес 30%
    if (subject.nearestPublicTransport != null) {
      const score = this.scoreDistance(subject.nearestPublicTransport);
      totalScore += score * 0.30;
      count += 0.30;
      if (score >= 8) factors.push(`транспорт (${subject.nearestPublicTransport}м)`);
    }

    // 2. Школа/садок - вес 25%
    if (subject.nearestSchool != null) {
      const score = this.scoreDistance(subject.nearestSchool);
      totalScore += score * 0.25;
      count += 0.25;
      if (score >= 8) factors.push(`школа (${subject.nearestSchool}м)`);
    }

    // 3. Супермаркет - вес 20%
    if (subject.nearestSupermarket != null) {
      const score = this.scoreDistance(subject.nearestSupermarket);
      totalScore += score * 0.20;
      count += 0.20;
      if (score >= 8) factors.push(`супермаркет (${subject.nearestSupermarket}м)`);
    }

    // 4. Больница/клиника - вес 15%
    if (subject.nearestHospital != null) {
      const score = this.scoreDistance(subject.nearestHospital);
      totalScore += score * 0.15;
      count += 0.15;
      if (score >= 8) factors.push(`лікарня (${subject.nearestHospital}м)`);
    }

    // Низкий приоритет

    // 5. Парковка - вес 10%
    if (subject.nearestParking != null) {
      const score = this.scoreDistance(subject.nearestParking);
      totalScore += score * 0.10;
      count += 0.10;
    }

    // Нормализация
    const finalScore = count > 0 ? totalScore / count : 5;

    const explanation = factors.length > 0
      ? `Близько: ${factors.join(', ')}`
      : 'Інфраструктура на середній відстані';

    return this.createResult(finalScore, explanation);
  }

  /**
   * Оценка расстояния до объекта инфраструктуры.
   * Чем ближе, тем выше балл (0-10).
   */
  private scoreDistance(distanceMeters: number): number {
    if (distanceMeters <= this.DISTANCE_THRESHOLDS.excellent) {
      // 0-300м: 9-10 баллов
      return 10 - (distanceMeters / this.DISTANCE_THRESHOLDS.excellent);
    }

    if (distanceMeters <= this.DISTANCE_THRESHOLDS.good) {
      // 300-500м: 7-9 баллов
      const ratio = (distanceMeters - this.DISTANCE_THRESHOLDS.excellent) /
                    (this.DISTANCE_THRESHOLDS.good - this.DISTANCE_THRESHOLDS.excellent);
      return 9 - ratio * 2;
    }

    if (distanceMeters <= this.DISTANCE_THRESHOLDS.average) {
      // 500-800м: 5-7 баллов
      const ratio = (distanceMeters - this.DISTANCE_THRESHOLDS.good) /
                    (this.DISTANCE_THRESHOLDS.average - this.DISTANCE_THRESHOLDS.good);
      return 7 - ratio * 2;
    }

    if (distanceMeters <= this.DISTANCE_THRESHOLDS.belowAverage) {
      // 800-1200м: 3-5 баллов
      const ratio = (distanceMeters - this.DISTANCE_THRESHOLDS.average) /
                    (this.DISTANCE_THRESHOLDS.belowAverage - this.DISTANCE_THRESHOLDS.average);
      return 5 - ratio * 2;
    }

    // > 1200м: 1-3 балла (пропорционально, минимум 1)
    const beyondRatio = Math.min((distanceMeters - this.DISTANCE_THRESHOLDS.belowAverage) / 800, 1);
    return Math.max(1, 3 - beyondRatio * 2);
  }
}
