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

// Ваги критеріїв ліквідності.
// Видалено: location (0.07), windows (0.05) — немає даних.
// Звільнені 0.12 перерозподілено. Ваги нормалізуються автоматично: L = Σ(Si*Wi) / Σ(Wi)
export const LIQUIDITY_WEIGHTS = {
  price: 0.23,            // Ціна (менше = краще, min-max)
  condition: 0.10,        // Стан / ремонт
  exposureTime: 0.09,     // Час експозиції
  furniture: 0.08,        // Меблі та техніка
  format: 0.08,           // Планування / кімнати
  infrastructure: 0.08,   // Інфраструктура (Overpass)
  floor: 0.07,            // Поверх
  uniqueFeatures: 0.07,   // Унікальні переваги
  communications: 0.06,   // Комунікації
  houseType: 0.05,        // Тип будинку
  competition: 0.05,      // Конкуренція / попит-пропозиція
  buyConditions: 0.04,    // Умови купівлі
  livingArea: 0.03,       // Жила площа (більше = краще, min-max)
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
