import { Injectable, Logger } from '@nestjs/common';
import { Street, StreetRepository } from '@libs/database';
import { MultiLanguageDto } from '@libs/common';

export type StreetMatchMethod = 'text_parsed' | 'text_found' | 'nearest';

export interface StreetMatchResult {
  streetId: number | null;
  matchMethod: StreetMatchMethod;
  confidence: number; // 0-1, how confident we are in the match
}

interface StreetCandidate {
  street: Street;
  distanceMeters: number;
}

interface ParsedStreet {
  name: string;
  type?: string; // вулиця, проспект, провулок, etc.
}

@Injectable()
export class StreetMatcherService {
  private readonly logger = new Logger(StreetMatcherService.name);

  // Regex patterns for street name extraction
  private readonly STREET_PATTERNS = [
    // Українська - з типом
    { regex: /(?:вул(?:иця)?\.?\s+)([А-ЯІЇЄҐа-яіїєґ][А-ЯІЇЄҐа-яіїєґ\s\-']+)/gi, type: 'вулиця' },
    { regex: /(?:просп(?:ект)?\.?\s+)([А-ЯІЇЄҐа-яіїєґ][А-ЯІЇЄҐа-яіїєґ\s\-']+)/gi, type: 'проспект' },
    { regex: /(?:пров(?:улок)?\.?\s+)([А-ЯІЇЄҐа-яіїєґ][А-ЯІЇЄҐа-яіїєґ\s\-']+)/gi, type: 'провулок' },
    { regex: /(?:бульв(?:ар)?\.?\s+)([А-ЯІЇЄҐа-яіїєґ][А-ЯІЇЄҐа-яіїєґ\s\-']+)/gi, type: 'бульвар' },
    { regex: /(?:пл(?:оща)?\.?\s+)([А-ЯІЇЄҐа-яіїєґ][А-ЯІЇЄҐа-яіїєґ\s\-']+)/gi, type: 'площа' },
    { regex: /(?:набережна\.?\s+)([А-ЯІЇЄҐа-яіїєґ][А-ЯІЇЄҐа-яіїєґ\s\-']+)/gi, type: 'набережна' },
    { regex: /(?:узвіз\.?\s+)([А-ЯІЇЄҐа-яіїєґ][А-ЯІЇЄҐа-яіїєґ\s\-']+)/gi, type: 'узвіз' },

    // Російська
    { regex: /(?:ул(?:ица)?\.?\s+)([А-Яа-яЁё][А-Яа-яЁё\s\-]+)/gi, type: 'улица' },
    { regex: /(?:пр(?:оспект)?\.?\s+)([А-Яа-яЁё][А-Яа-яЁё\s\-]+)/gi, type: 'проспект' },
    { regex: /(?:пер(?:еулок)?\.?\s+)([А-Яа-яЁё][А-Яа-яЁё\s\-]+)/gi, type: 'переулок' },
    { regex: /(?:бульв(?:ар)?\.?\s+)([А-Яа-яЁё][А-Яа-яЁё\s\-]+)/gi, type: 'бульвар' },
    { regex: /(?:пл(?:ощадь)?\.?\s+)([А-Яа-яЁё][А-Яа-яЁё\s\-]+)/gi, type: 'площадь' },
    { regex: /(?:наб(?:ережная)?\.?\s+)([А-Яа-яЁё][А-Яа-яЁё\s\-]+)/gi, type: 'набережная' },
    { regex: /(?:спуск\.?\s+)([А-Яа-яЁё][А-Яа-яЁё\s\-]+)/gi, type: 'спуск' },
  ];

  // Common abbreviations to normalize
  private readonly ABBREVIATIONS: Record<string, string> = {
    'вул.': 'вулиця',
    'ул.': 'улица',
    'просп.': 'проспект',
    'пр.': 'проспект',
    'пров.': 'провулок',
    'пер.': 'переулок',
    'бульв.': 'бульвар',
    'б-р': 'бульвар',
    'пл.': 'площа',
    'наб.': 'набережна',
  };

  public constructor(private readonly streetRepository: StreetRepository) {}

  /**
   * Main method: resolve street for a listing using hybrid approach
   */
  public async resolveStreet(
    lng: number,
    lat: number,
    text?: string,
    geoId?: number,
  ): Promise<StreetMatchResult> {
    // 1. Get nearest street candidates
    const candidates = await this.streetRepository.findNearestStreets(lng, lat, geoId, 5, 500);

    if (candidates.length === 0) {
      // Try without geoId restriction
      const candidatesNoGeo = await this.streetRepository.findNearestStreets(lng, lat, undefined, 5, 500);
      if (candidatesNoGeo.length === 0) {
        return { streetId: null, matchMethod: 'nearest', confidence: 0 };
      }
      candidates.push(...candidatesNoGeo);
    }

    if (text) {
      // 2. Try to parse street name from text
      const parsed = this.extractStreetFromText(text);
      if (parsed) {
        const matched = this.fuzzyMatchStreet(parsed.name, candidates);
        if (matched) {
          this.logger.debug(
            `Street matched by parsing: "${parsed.name}" -> ${matched.street.name?.uk} (score: ${matched.score.toFixed(2)})`,
          );
          return {
            streetId: matched.street.id,
            matchMethod: 'text_parsed',
            confidence: matched.score,
          };
        }
      }

      // 3. Search for candidate names in text
      const foundInText = this.findStreetNameInText(candidates, text);
      if (foundInText) {
        this.logger.debug(`Street found in text: ${foundInText.street.name?.uk}`);
        return {
          streetId: foundInText.street.id,
          matchMethod: 'text_found',
          confidence: foundInText.confidence,
        };
      }
    }

    // 4. Fallback to nearest street
    const nearest = candidates[0];
    return {
      streetId: nearest.street.id,
      matchMethod: 'nearest',
      confidence: this.calculateDistanceConfidence(nearest.distanceMeters),
    };
  }

  /**
   * Extract street name from text using regex patterns
   */
  public extractStreetFromText(text: string): ParsedStreet | null {
    const normalizedText = this.normalizeText(text);

    for (const pattern of this.STREET_PATTERNS) {
      const matches = normalizedText.matchAll(pattern.regex);
      for (const match of matches) {
        if (match[1]) {
          const name = this.cleanStreetName(match[1]);
          if (name.length >= 3) {
            return { name, type: pattern.type };
          }
        }
      }
    }

    return null;
  }

  /**
   * Fuzzy match parsed street name against candidates
   */
  public fuzzyMatchStreet(
    parsedName: string,
    candidates: StreetCandidate[],
  ): { street: Street; score: number } | null {
    const normalizedParsed = this.normalizeStreetName(parsedName);
    let bestMatch: { street: Street; score: number } | null = null;

    for (const candidate of candidates) {
      const names = this.getStreetNames(candidate.street);

      for (const name of names) {
        const normalizedCandidate = this.normalizeStreetName(name);
        const score = this.calculateSimilarity(normalizedParsed, normalizedCandidate);

        // Distance bonus: closer streets get slight preference
        const distanceBonus = Math.max(0, 0.1 * (1 - candidate.distanceMeters / 500));
        const finalScore = score + distanceBonus;

        if (finalScore > 0.7 && (!bestMatch || finalScore > bestMatch.score)) {
          bestMatch = { street: candidate.street, score: finalScore };
        }
      }
    }

    return bestMatch;
  }

  /**
   * Search for any candidate street name in the text
   */
  public findStreetNameInText(
    candidates: StreetCandidate[],
    text: string,
  ): { street: Street; confidence: number } | null {
    const normalizedText = this.normalizeText(text).toLowerCase();

    // Sort by distance to prefer closer streets when multiple match
    const sortedCandidates = [...candidates].sort((a, b) => a.distanceMeters - b.distanceMeters);

    for (const candidate of sortedCandidates) {
      const names = this.getStreetNames(candidate.street);

      for (const name of names) {
        const normalizedName = this.normalizeStreetName(name).toLowerCase();
        if (normalizedName.length < 4) continue;

        // Check if name appears in text
        if (normalizedText.includes(normalizedName)) {
          // Calculate confidence based on match quality and distance
          const distanceConfidence = this.calculateDistanceConfidence(candidate.distanceMeters);
          const lengthBonus = Math.min(0.2, normalizedName.length / 50);
          const confidence = Math.min(1, 0.7 + lengthBonus + distanceConfidence * 0.1);

          return { street: candidate.street, confidence };
        }
      }
    }

    return null;
  }

  /**
   * Get all name variants for a street
   */
  private getStreetNames(street: Street): string[] {
    const names: string[] = [];

    if (street.name) {
      if (street.name.uk) names.push(street.name.uk);
      if (street.name.ru) names.push(street.name.ru);
      if (street.name.en) names.push(street.name.en);
    }

    if (street.names) {
      Object.values(street.names).forEach((name) => {
        if (name && typeof name === 'string') names.push(name);
      });
    }

    if (street.alias) names.push(street.alias);

    return names.filter((n) => n && n.length > 0);
  }

  /**
   * Normalize text for matching
   */
  private normalizeText(text: string): string {
    if (!text) return '';

    // Handle JSON description
    if (text.startsWith('{')) {
      try {
        const parsed = JSON.parse(text) as MultiLanguageDto;
        const parts: string[] = [];
        if (parsed.uk) parts.push(parsed.uk);
        if (parsed.ru) parts.push(parsed.ru);
        if (parsed.en) parts.push(parsed.en);
        return parts.join(' ');
      } catch {
        // Not valid JSON, use as-is
      }
    }

    return text;
  }

  /**
   * Normalize street name for comparison
   */
  private normalizeStreetName(name: string): string {
    if (!name) return '';

    return name
      .toLowerCase()
      .replace(/[''`]/g, "'")
      .replace(/\s+/g, ' ')
      .replace(/^\s+|\s+$/g, '')
      .replace(/\d+.*$/, '') // Remove house numbers at the end
      .trim();
  }

  /**
   * Clean extracted street name
   */
  private cleanStreetName(name: string): string {
    return name
      .replace(/\s*,.*$/, '') // Remove everything after comma
      .replace(/\s*\d+.*$/, '') // Remove house numbers
      .replace(/^\s+|\s+$/g, '')
      .trim();
  }

  /**
   * Calculate string similarity using Levenshtein-based approach
   */
  private calculateSimilarity(str1: string, str2: string): number {
    if (!str1 || !str2) return 0;
    if (str1 === str2) return 1;

    const len1 = str1.length;
    const len2 = str2.length;

    // Quick check for very different lengths
    if (Math.abs(len1 - len2) > Math.max(len1, len2) * 0.5) {
      return 0;
    }

    // Check if one contains the other
    if (str1.includes(str2) || str2.includes(str1)) {
      return 0.9;
    }

    // Levenshtein distance
    const matrix: number[][] = [];

    for (let i = 0; i <= len1; i++) {
      matrix[i] = [i];
    }
    for (let j = 0; j <= len2; j++) {
      matrix[0][j] = j;
    }

    for (let i = 1; i <= len1; i++) {
      for (let j = 1; j <= len2; j++) {
        const cost = str1[i - 1] === str2[j - 1] ? 0 : 1;
        matrix[i][j] = Math.min(
          matrix[i - 1][j] + 1,
          matrix[i][j - 1] + 1,
          matrix[i - 1][j - 1] + cost,
        );
      }
    }

    const distance = matrix[len1][len2];
    const maxLen = Math.max(len1, len2);

    return 1 - distance / maxLen;
  }

  /**
   * Calculate confidence based on distance to street
   */
  private calculateDistanceConfidence(distanceMeters: number): number {
    // 0-50m: high confidence, 50-200m: medium, 200-500m: low
    if (distanceMeters <= 50) return 0.9;
    if (distanceMeters <= 100) return 0.7;
    if (distanceMeters <= 200) return 0.5;
    if (distanceMeters <= 350) return 0.3;
    return 0.1;
  }
}
