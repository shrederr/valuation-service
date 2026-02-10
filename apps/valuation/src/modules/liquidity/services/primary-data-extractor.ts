import { Injectable } from '@nestjs/common';
import { UnifiedListing } from '@libs/database';

/**
 * Универсальный парсер primaryData для разных платформ (OLX, domRia, realtorUa).
 * Извлекает структурированные данные из JSONB поля primaryData.
 */
@Injectable()
export class PrimaryDataExtractor {
  /**
   * Извлекает данные о мебели.
   * OLX: param key="furnish", values: "Так"/"Ні"
   */
  public extractFurnish(listing: UnifiedListing): 'yes' | 'no' | 'partial' | null {
    const value = this.extractOlxParam(listing.primaryData, 'furnish');
    if (!value) return null;

    const lower = value.toLowerCase();
    if (lower === 'так' || lower === 'yes' || lower === 'да') return 'yes';
    if (lower === 'ні' || lower === 'no' || lower === 'нет') return 'no';
    if (lower === 'частково' || lower === 'partial') return 'partial';

    return null;
  }

  /**
   * Извлекает коммуникации.
   * OLX: param key="communications", value="electricity,water,gas,heating,sewerage"
   */
  public extractCommunications(listing: UnifiedListing): string[] | null {
    const value = this.extractOlxParam(listing.primaryData, 'communications');
    if (!value) return null;

    const items = value.split(',').map((s) => s.trim().toLowerCase()).filter(Boolean);
    return items.length > 0 ? items : null;
  }

  /**
   * Извлекает comfort-теги.
   * OLX: param key="comfort", value="elevator,intercom,parking,balcony,closed_area,panoramic_windows..."
   */
  public extractComfort(listing: UnifiedListing): string[] | null {
    const value = this.extractOlxParam(listing.primaryData, 'comfort');
    if (!value) return null;

    const items = value.split(',').map((s) => s.trim().toLowerCase()).filter(Boolean);
    return items.length > 0 ? items : null;
  }

  /**
   * Извлекает планировку.
   * OLX: param key="layout", values: "Роздільна", "Студія", "Суміжно-роздільна", etc.
   */
  public extractLayout(listing: UnifiedListing): string | null {
    return this.extractOlxParam(listing.primaryData, 'layout');
  }

  /**
   * Извлекает условия покупки из описания (text search).
   * Ищет: єОселя, іпотека, розтерміновка, рассрочка, торг, обмін
   */
  public extractBuyConditions(listing: UnifiedListing): string[] | null {
    const description = this.getDescriptionText(listing);
    if (!description) return null;

    const lower = description.toLowerCase();
    const conditions: string[] = [];

    if (lower.includes('єоселя') || lower.includes('єосел')) {
      conditions.push('eOselya');
    }
    if (lower.includes('іпотек') || lower.includes('ипотек')) {
      conditions.push('mortgage');
    }
    if (lower.includes('розтерміновк') || lower.includes('рассрочк') || lower.includes('розстрочк')) {
      conditions.push('installment');
    }
    if (lower.includes('торг')) {
      conditions.push('bargain');
    }
    if (lower.includes('обмін') || lower.includes('обмен')) {
      conditions.push('exchange');
    }

    return conditions.length > 0 ? conditions : null;
  }

  /**
   * Извлекает значение OLX-параметра из primaryData.
   * OLX формат: { params: [{ key: "furnish", value: "Так", normalizedValue: "yes" }, ...] }
   */
  private extractOlxParam(primaryData: Record<string, unknown> | undefined, key: string): string | null {
    if (!primaryData) return null;

    const params = primaryData.params;
    if (!Array.isArray(params)) return null;

    const param = params.find((p: Record<string, unknown>) => p && p.key === key);
    if (!param) return null;

    const value = (param as Record<string, unknown>).value ?? (param as Record<string, unknown>).normalizedValue;
    return typeof value === 'string' ? value : null;
  }

  /**
   * Получает текст описания из разных форматов.
   */
  private getDescriptionText(listing: UnifiedListing): string | null {
    if (!listing.description) return null;

    if (typeof listing.description === 'string') {
      return listing.description;
    }

    // MultiLanguageDto: { uk?: string, ru?: string }
    const desc = listing.description as unknown as { uk?: string; ru?: string };
    return desc.uk || desc.ru || null;
  }
}
