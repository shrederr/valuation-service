import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { DataSource } from 'typeorm';
import { UnifiedListing } from '@libs/database';
import { PrimaryDataExtractor } from './primary-data-extractor';
import OpenAI from 'openai';

export interface PhotoCompareResult {
  verdict: 'SAME' | 'DIFFERENT' | 'UNCERTAIN' | 'ERROR';
  confidence: number;
  reasoning: string;
}

const SYSTEM_PROMPT = `You are a real estate duplicate detection expert.
You will be shown photos from two different property listings (LISTING A and LISTING B).
Your task is to determine if they show the SAME physical apartment/house/property or DIFFERENT ones.

Focus on PERMANENT structural features:
- Room layouts and proportions
- Wall colors, wallpaper patterns
- Flooring type and color
- Kitchen cabinets, countertops, backsplash
- Bathroom tiles, fixtures, bathtub/shower type
- Window frames, views from windows
- Door styles, ceiling features (moldings, height)
- Balcony/loggia appearance

IGNORE temporary/movable items: furniture, curtains, decorations, appliances.
Photos may be taken at different times, angles, or lighting conditions by different people.

Respond ONLY with valid JSON (no markdown, no code blocks):
{"verdict":"SAME","confidence":0.85,"reasoning":"Brief explanation"}

verdict must be one of: "SAME", "DIFFERENT", "UNCERTAIN"
confidence must be a number between 0.0 and 1.0`;

@Injectable()
export class PhotoDedupService {
  private readonly logger = new Logger(PhotoDedupService.name);
  private readonly openai: OpenAI | null;
  private readonly enabled: boolean;
  private readonly model: string;
  private readonly maxPhotos = 4;

  private stats = { calls: 0, errors: 0, same: 0, different: 0, uncertain: 0 };

  constructor(
    private readonly configService: ConfigService,
    private readonly dataSource: DataSource,
    private readonly primaryDataExtractor: PrimaryDataExtractor,
  ) {
    const apiKey = this.configService.get<string>('OPENAI_API_KEY', '');
    this.model = this.configService.get<string>('PHOTO_DEDUP_MODEL', 'gpt-4o');
    this.enabled = !!apiKey;

    if (this.enabled) {
      this.openai = new OpenAI({ apiKey });
      this.logger.log(`PhotoDedupService initialized with model ${this.model}`);
    } else {
      this.openai = null;
      this.logger.warn('PhotoDedupService disabled: OPENAI_API_KEY not set');
    }
  }

  isEnabled(): boolean {
    return this.enabled;
  }

  getStats() {
    return { ...this.stats, enabled: this.enabled };
  }

  /**
   * Compare photos of two listings via GPT-4o Vision.
   * Returns ERROR if can't determine (API fail, no photos, etc.) — caller must handle.
   */
  async compare(
    listingA: UnifiedListing,
    listingB: UnifiedListing,
  ): Promise<PhotoCompareResult> {
    if (!this.openai) {
      return { verdict: 'ERROR', confidence: 0, reasoning: 'Photo dedup disabled: no API key' };
    }

    // Extract photos
    const photosA = this.extractPhotos(listingA);
    const photosB = this.extractPhotos(listingB);

    if (!photosA || photosA.length < 2) {
      return { verdict: 'ERROR', confidence: 0, reasoning: `Listing A has insufficient photos (${photosA?.length || 0})` };
    }
    if (!photosB || photosB.length < 2) {
      return { verdict: 'ERROR', confidence: 0, reasoning: `Listing B has insufficient photos (${photosB?.length || 0})` };
    }

    // Limit to maxPhotos each
    const selectedA = photosA.slice(0, this.maxPhotos);
    const selectedB = photosB.slice(0, this.maxPhotos);

    this.stats.calls++;
    const startTime = Date.now();

    try {
      const result = await this.callGptVision(selectedA, selectedB);
      const elapsed = Date.now() - startTime;

      this.logger.log(
        `Photo compare ${listingA.sourceId} vs ${listingB.sourceId}: ` +
        `${result.verdict} (confidence=${result.confidence.toFixed(2)}, ${elapsed}ms) — ${result.reasoning}`,
      );

      if (result.verdict === 'SAME') this.stats.same++;
      else if (result.verdict === 'DIFFERENT') this.stats.different++;
      else this.stats.uncertain++;

      return result;
    } catch (error) {
      this.stats.errors++;
      const message = error instanceof Error ? error.message : String(error);
      this.logger.error(`Photo compare API error: ${message}`);
      return { verdict: 'ERROR', confidence: 0, reasoning: `API error: ${message}` };
    }
  }

  /**
   * Extract photo URLs from listing (via PrimaryDataExtractor).
   */
  private extractPhotos(listing: UnifiedListing): string[] | null {
    const data = this.primaryDataExtractor.extractForExport(listing);
    if (!data.photos || data.photos.length === 0) return null;

    // Filter out invalid URLs
    return data.photos.filter(url =>
      typeof url === 'string' && url.startsWith('http') && url.length > 10,
    );
  }

  /**
   * Call GPT-4o Vision API with photos from both listings.
   */
  private async callGptVision(
    photosA: string[],
    photosB: string[],
  ): Promise<PhotoCompareResult> {
    // Build message content with images
    const content: OpenAI.Chat.Completions.ChatCompletionContentPart[] = [
      { type: 'text', text: `LISTING A (${photosA.length} photos):` },
    ];

    for (const url of photosA) {
      content.push({
        type: 'image_url',
        image_url: { url, detail: 'low' },
      });
    }

    content.push({ type: 'text', text: `LISTING B (${photosB.length} photos):` });

    for (const url of photosB) {
      content.push({
        type: 'image_url',
        image_url: { url, detail: 'low' },
      });
    }

    const response = await this.openai!.chat.completions.create({
      model: this.model,
      messages: [
        { role: 'system', content: SYSTEM_PROMPT },
        { role: 'user', content },
      ],
      max_tokens: 200,
      temperature: 0.1,
    });

    const text = response.choices[0]?.message?.content?.trim();
    if (!text) {
      throw new Error('Empty GPT response');
    }

    return this.parseResponse(text);
  }

  /**
   * Parse GPT response JSON. Handles possible markdown code blocks.
   */
  private parseResponse(text: string): PhotoCompareResult {
    // Strip markdown code blocks if present
    let cleaned = text;
    if (cleaned.startsWith('```')) {
      cleaned = cleaned.replace(/^```(?:json)?\n?/, '').replace(/\n?```$/, '');
    }

    try {
      const parsed = JSON.parse(cleaned);

      const verdict = parsed.verdict?.toUpperCase();
      if (!['SAME', 'DIFFERENT', 'UNCERTAIN'].includes(verdict)) {
        throw new Error(`Invalid verdict: ${parsed.verdict}`);
      }

      const confidence = typeof parsed.confidence === 'number'
        ? Math.max(0, Math.min(1, parsed.confidence))
        : 0.5;

      const reasoning = typeof parsed.reasoning === 'string'
        ? parsed.reasoning.substring(0, 500)
        : 'No reasoning provided';

      return { verdict, confidence, reasoning };
    } catch (error) {
      this.logger.warn(`Failed to parse GPT response: ${text}`);
      throw new Error(`Invalid GPT response: ${text.substring(0, 200)}`);
    }
  }

  /**
   * Load a listing by ID (for loading candidate during comparison).
   */
  async loadListing(id: string): Promise<UnifiedListing | null> {
    const rows = await this.dataSource.query(
      `SELECT * FROM unified_listings WHERE id = $1`,
      [id],
    );
    if (rows.length === 0) return null;
    const raw = rows[0];
    // Map snake_case DB columns to camelCase properties
    if (raw.primary_data && !raw.primaryData) {
      raw.primaryData = raw.primary_data;
    }
    return raw;
  }
}
