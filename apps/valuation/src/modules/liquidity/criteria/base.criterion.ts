import { UnifiedListing } from '@libs/database';
import { FairPriceDto } from '@libs/models';

export interface CriterionResult {
  name: string;
  weight: number;
  score: number;
  weightedScore: number;
  explanation?: string;
}

export interface ExposureStats {
  medianDays: number;
  avgDays: number;
  count: number;
}

export interface CriterionContext {
  subject: UnifiedListing;
  fairPrice?: FairPriceDto;
  analogs?: UnifiedListing[];
  exposureStats?: ExposureStats | null;
}

export abstract class BaseCriterion {
  public abstract readonly name: string;
  public abstract readonly weight: number;

  public abstract evaluate(context: CriterionContext): CriterionResult;

  protected createResult(score: number, explanation?: string): CriterionResult {
    const normalizedScore = Math.max(0, Math.min(10, score));

    return {
      name: this.name,
      weight: this.weight,
      score: normalizedScore,
      weightedScore: normalizedScore * this.weight,
      explanation,
    };
  }

  /**
   * Создает результат с нулевым весом для случаев когда нет данных.
   * Такой критерий не влияет на итоговый скоринг.
   */
  protected createNullResult(explanation: string): CriterionResult {
    return {
      name: this.name,
      weight: 0, // Нулевой вес - не влияет на итоговый скоринг
      score: 0,
      weightedScore: 0,
      explanation: `⚠️ ${explanation}`,
    };
  }
}

// Веса согласно ТЗ (Оценка ликвидности.xlsx)
export const LIQUIDITY_WEIGHTS = {
  price: 0.2, // Цена
  livingArea: 0.03,      // Жилая площадь (больше = лучше, min-max нормализация)
  exposureTime: 0.08,    // Среднее время экспозиции на рынке
  competition: 0.05,     // Соотношение спроса и предложения
  location: 0.07,        // Вид из окон + природа (0.04 + 0.03)
  infrastructure: 0.07,  // Поруч (инфраструктура) - row 38 в ТЗ
  condition: 0.09,       // Общее состояние
  format: 0.07,          // Количество комнат + планировка
  floor: 0.06,           // Этаж
  houseType: 0.05,       // Тип здания
  furniture: 0.07,       // Мебель и техника
  windows: 0.05,         // Тип окон (не реализован - нет данных)
  uniqueFeatures: 0.06,  // Уникальные преимущества
  buyConditions: 0.04,   // Условия покупки
  communications: 0.05,  // Коммуникации
};

// Медианное время продажи по типам (из реальных данных)
export const MEDIAN_DAYS_TO_SELL = {
  apartment: 30,
  house: 58,
  commercial: 51,
  area: 68,
  room: 30,
  garage: 45,
  default: 45,
};
