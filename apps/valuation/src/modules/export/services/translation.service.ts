import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { HttpService } from '@nestjs/axios';
import { firstValueFrom } from 'rxjs';
import { UnifiedListing } from '@libs/database';

@Injectable()
export class TranslationService {
  private readonly logger = new Logger(TranslationService.name);
  private readonly apiUrl: string;
  private readonly apiKey: string;
  private readonly enabled: boolean;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
  ) {
    this.apiUrl = this.configService.get<string>('TRANSLATE_API_URL', '');
    this.apiKey = this.configService.get<string>('TRANSLATE_API_KEY', '');
    this.enabled = !!this.apiUrl && !!this.apiKey;

    if (this.enabled) {
      this.logger.log(`Translation service configured: ${this.apiUrl}`);
    } else {
      this.logger.warn('TRANSLATE_API_URL or TRANSLATE_API_KEY not set — translations disabled');
    }
  }

  /**
   * Translate text to target language via text-generator-ai service.
   * Returns null on error (non-blocking).
   */
  async translate(text: string, targetLang: 'uk' | 'ru'): Promise<string | null> {
    if (!this.enabled) return null;

    // API limit: 3000 chars
    const truncated = text.length > 3000 ? text.slice(0, 3000) : text;

    try {
      const response = await firstValueFrom(
        this.httpService.post(
          `${this.apiUrl}/text/translate`,
          { targetLang, text: truncated },
          {
            headers: { 'X-Api-Key': this.apiKey },
            timeout: 20000,
          },
        ),
      );

      const translated = response.data?.text;
      if (typeof translated === 'string' && translated.trim()) {
        return translated.trim();
      }

      this.logger.warn(`Empty translation response for targetLang=${targetLang}`);
      return null;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      this.logger.warn(`Translation failed (targetLang=${targetLang}): ${msg}`);
      return null;
    }
  }

  /**
   * Ensure listing has both UK and RU descriptions.
   * Returns updated description object, or original if no translation needed/possible.
   */
  async ensureTranslations(
    listing: UnifiedListing,
  ): Promise<{ uk: string; ru?: string; en?: string } | undefined> {
    const desc = listing.description;
    if (!desc) return undefined;

    const hasUk = !!desc.uk?.trim();
    const hasRu = !!desc.ru?.trim();

    // Both languages present — nothing to do
    if (hasUk && hasRu) return desc;

    // No text at all
    if (!hasUk && !hasRu) return desc;

    const result = { ...desc };

    if (hasUk && !hasRu) {
      // Translate UK → RU
      const translated = await this.translate(desc.uk, 'ru');
      if (translated) {
        result.ru = translated;
      }
    } else if (hasRu && !hasUk) {
      // Translate RU → UK
      const translated = await this.translate(desc.ru!, 'uk');
      if (translated) {
        result.uk = translated;
      }
    }

    return result;
  }

  isEnabled(): boolean {
    return this.enabled;
  }
}
