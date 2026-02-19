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
   * Vector2: attributes.furniture = 1 (Мебльована) / 2 (Без меблів)
   */
  public extractFurnish(listing: UnifiedListing): 'yes' | 'no' | 'partial' | null {
    // OLX format
    const value = this.extractOlxParam(listing.primaryData, 'furnish');
    if (value) {
      const lower = value.toLowerCase();
      if (lower === 'так' || lower === 'yes' || lower === 'да') return 'yes';
      if (lower === 'ні' || lower === 'no' || lower === 'нет') return 'no';
      if (lower === 'частково' || lower === 'partial') return 'partial';
    }

    // Vector2 format: attributes.furniture (1=yes, 2=no)
    const attrs = listing.attributes;
    if (attrs?.furniture !== undefined) {
      const furn = Number(attrs.furniture);
      if (furn === 1) return 'yes';
      if (furn === 2) return 'no';
    }

    return null;
  }

  /**
   * Извлекает коммуникации.
   * OLX: param key="communications", value="electricity,water,gas,heating,sewerage"
   * Vector2: attributes.gas_type, water_type, electricity_type, sewerage_type, heating_type
   */
  public extractCommunications(listing: UnifiedListing): string[] | null {
    // OLX format
    const value = this.extractOlxParam(listing.primaryData, 'communications');
    if (value) {
      const items = value.split(',').map((s) => s.trim().toLowerCase()).filter(Boolean);
      if (items.length > 0) return items;
    }

    // Vector2 format: individual type fields (value > 0 means present, some values mean "absent")
    const attrs = listing.attributes;
    if (!attrs) return null;

    const comms: string[] = [];
    // electricity_type: 1=220, 2=380, 3=Без електрики
    if (attrs.electricity_type !== undefined && Number(attrs.electricity_type) !== 3) {
      comms.push('electricity');
    }
    // water_type: 5=Без водопроводу, 13=Привозна вода — both mean no piped water
    const waterType = Number(attrs.water_type);
    if (attrs.water_type !== undefined && waterType !== 5 && waterType !== 13) {
      comms.push('water');
    }
    // gas_type: 3=Ні, rest = has gas
    if (attrs.gas_type !== undefined && Number(attrs.gas_type) !== 3) {
      comms.push('gas');
    }
    // heating_type: 6=Без опалення, rest = has heating
    if (attrs.heating_type !== undefined && Number(attrs.heating_type) !== 6) {
      comms.push('heating');
    }
    // sewerage_type: 3=Без каналізації, rest = has sewerage
    if (attrs.sewerage_type !== undefined && Number(attrs.sewerage_type) !== 3) {
      comms.push('sewerage');
    }

    return comms.length > 0 ? comms : null;
  }

  /**
   * Извлекает comfort-теги.
   * OLX: param key="comfort", value="elevator,intercom,parking,balcony,closed_area,panoramic_windows..."
   * Vector2: attributes.balcony_type, windows_face, parking, etc.
   */
  public extractComfort(listing: UnifiedListing): string[] | null {
    // OLX format
    const value = this.extractOlxParam(listing.primaryData, 'comfort');
    if (value) {
      const items = value.split(',').map((s) => s.trim().toLowerCase()).filter(Boolean);
      if (items.length > 0) return items;
    }

    // Vector2 format: individual attribute fields
    const attrs = listing.attributes;
    if (!attrs) return null;

    const comfort: string[] = [];
    // balcony_type: 1=ні, 2+=балкон/лоджія
    const balcony = Number(attrs.balcony_type);
    if (balcony >= 2) {
      if (balcony === 4 || balcony === 5 || balcony === 11 || balcony === 15) {
        comfort.push('loggia');
      } else {
        comfort.push('balcony');
      }
    }
    // windows_face: 9=Море, 11=Парк — panoramic-like
    const windowsFace = Number(attrs.windows_face);
    if (windowsFace === 9 || windowsFace === 11) {
      comfort.push('panoramic_windows');
    }
    // parking: 1=Нет, 2=Закрепленный, 3=Свободная, 4=Подземный, 5=Наземный, 6=Гараж
    if (attrs.parking !== undefined && Number(attrs.parking) >= 2) {
      comfort.push('parking');
    }

    return comfort.length > 0 ? comfort : null;
  }

  /**
   * Извлекает планировку.
   * OLX: param key="layout", values: "Роздільна", "Студія", "Суміжно-роздільна", etc.
   */
  public extractLayout(listing: UnifiedListing): string | null {
    return this.extractOlxParam(listing.primaryData, 'layout');
  }

  /**
   * Извлекает условия покупки.
   * OLX/описание: парсит єОселя, іпотека, розтерміновка, торг, обмін
   * Vector2: attributes.bargain, credit_eoselya, in_installments, special_condition_sale, description
   */
  public extractBuyConditions(listing: UnifiedListing): string[] | null {
    const conditions: string[] = [];

    // Vector2 structured attributes
    const attrs = listing.attributes;
    if (attrs) {
      // credit_eoselya: 1=Підходить, 2=Не підходить
      if (Number(attrs.credit_eoselya) === 1) {
        conditions.push('eOselya');
      }
      // special_condition_sale: 1=Іпотека банку
      if (Number(attrs.special_condition_sale) === 1) {
        conditions.push('mortgage');
      }
      // in_installments: 1=Так, 2=Ні
      if (Number(attrs.in_installments) === 1) {
        conditions.push('installment');
      }
      if (attrs.bargain && Number(attrs.bargain) > 0) {
        conditions.push('bargain');
      }
    }

    // Text search in description (OLX primaryData or vector2 attributes.description)
    const description = this.getDescriptionText(listing);
    const attrDesc = attrs?.description as string | undefined;
    const attrDescRekl = attrs?.description_rekl_ua as string | undefined;
    const allText = [description, attrDesc, attrDescRekl].filter(Boolean).join(' ').toLowerCase();

    if (allText) {
      if (!conditions.includes('eOselya') && (allText.includes('єоселя') || allText.includes('єосел'))) {
        conditions.push('eOselya');
      }
      if (!conditions.includes('mortgage') && (allText.includes('іпотек') || allText.includes('ипотек'))) {
        conditions.push('mortgage');
      }
      if (!conditions.includes('installment') && (allText.includes('розтерміновк') || allText.includes('рассрочк') || allText.includes('розстрочк'))) {
        conditions.push('installment');
      }
      if (!conditions.includes('bargain') && allText.includes('торг')) {
        conditions.push('bargain');
      }
      if (!conditions.includes('exchange') && (allText.includes('обмін') || allText.includes('обмен'))) {
        conditions.push('exchange');
      }
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
