import { Injectable, OnModuleInit } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { ApartmentComplex as ApartmentComplexEntity } from '@libs/database';

interface ApartmentComplex {
  id: number;
  osmId?: number;
  nameRu: string;
  nameUk: string;
  nameEn?: string;
  nameNormalized: string;
  lat: number;
  lng: number;
  hasPolygon: boolean;
  geoId?: number;
  streetId?: number;
}

interface MatchResult {
  complexId: number;
  complexName: string;
  matchedText: string;
  score: number;
  lat: number;
  lng: number;
  hasPolygon: boolean;
}

@Injectable()
export class ComplexMatcherService implements OnModuleInit {
  private complexes: ApartmentComplex[] = [];
  private searchPatterns: Map<number, RegExp[]> = new Map();

  constructor(
    @InjectRepository(ApartmentComplexEntity)
    private readonly complexRepository: Repository<any>,
  ) {}

  async onModuleInit() {
    await this.loadComplexes();
  }

  /**
   * Load all complexes and build search patterns
   */
  async loadComplexes(): Promise<void> {
    const rows = await this.complexRepository.query(`
      SELECT
        id, osm_id, name_ru, name_uk, name_en, name_normalized,
        lat, lng, polygon IS NOT NULL as has_polygon,
        geo_id, street_id
      FROM apartment_complexes
      ORDER BY LENGTH(name_normalized) DESC
    `);

    this.complexes = rows.map((r: any) => ({
      id: r.id,
      osmId: r.osm_id,
      nameRu: r.name_ru,
      nameUk: r.name_uk,
      nameEn: r.name_en,
      nameNormalized: r.name_normalized,
      lat: parseFloat(r.lat),
      lng: parseFloat(r.lng),
      hasPolygon: r.has_polygon,
      geoId: r.geo_id,
      streetId: r.street_id,
    }));

    // Build search patterns for each complex
    for (const complex of this.complexes) {
      const patterns = this.buildSearchPatterns(complex);
      this.searchPatterns.set(complex.id, patterns);
    }

    console.log(`ComplexMatcherService: Loaded ${this.complexes.length} complexes`);
  }

  /**
   * Build regex patterns for matching complex name in text
   */
  private buildSearchPatterns(complex: ApartmentComplex): RegExp[] {
    const patterns: RegExp[] = [];
    const names = [complex.nameRu, complex.nameUk, complex.nameEn].filter(Boolean);

    for (const name of names) {
      // Extract meaningful part (remove ЖК, etc.)
      const cleaned = this.cleanName(name as string);
      if (cleaned.length < 3) continue;

      // Pattern 1: Exact match with ЖК/КГ prefix
      patterns.push(new RegExp(
        `(?:жк|жилой комплекс|житловий комплекс|кг|км|коттеджный городок|котеджне містечко)?\\s*["«']?${this.escapeRegex(cleaned)}["»']?`,
          'gi',
      ));

      // Pattern 2: Just the name
      if (cleaned.length >= 4) {
        patterns.push(new RegExp(
          `\\b${this.escapeRegex(cleaned)}\\b`,
          'gi'
        ));
      }

      // Pattern 3: For multi-word names, also match partial
      const words = cleaned.split(/\s+/).filter(w => w.length >= 3);
      if (words.length >= 2) {
        // Match any 2 consecutive words
        for (let i = 0; i < words.length - 1; i++) {
          patterns.push(new RegExp(
            `\\b${this.escapeRegex(words[i])}\\s+${this.escapeRegex(words[i + 1])}\\b`,
            'gi'
          ));
        }
      }
    }

    return patterns;
  }

  /**
   * Clean name for matching (remove common prefixes)
   */
  private cleanName(name: string): string {
    return name
      .replace(/^(жк|жилой комплекс|житловий комплекс|кг|км|котеджне|коттеджное|містечко|городок|таунхаус[иі]?|дуплекс[иі]?)\s*/gi, '')
      .replace(/["«»'']/g, '')
      .replace(/\s*\([^)]+\)\s*/g, ' ') // Remove parentheses content
      .replace(/\s*буд\.?\s*\d+/gi, '') // Remove building numbers
      .trim();
  }

  /**
   * Escape special regex characters
   */
  private escapeRegex(str: string): string {
    return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }

  /**
   * Find complex by matching name in text (title + description)
   */
  findComplexInText(title: string, description?: string): MatchResult | null {
    const text = `${title || ''} ${description || ''}`.toLowerCase();
    if (!text.trim()) return null;

    let bestMatch: MatchResult | null = null;
    let bestScore = 0;

    for (const complex of this.complexes) {
      const patterns = this.searchPatterns.get(complex.id);
      if (!patterns) continue;

      for (const pattern of patterns) {
        const match = text.match(pattern);
        if (match) {
          // Calculate score based on match length and position
          const matchedText = match[0];
          const score = this.calculateMatchScore(matchedText, complex.nameNormalized, text);

          if (score > bestScore) {
            bestScore = score;
            bestMatch = {
              complexId: complex.id,
              complexName: complex.nameRu,
              matchedText,
              score,
              lat: complex.lat,
              lng: complex.lng,
              hasPolygon: complex.hasPolygon,
            };
          }
        }
      }
    }

    // Only return if score is good enough
    return bestScore >= 0.5 ? bestMatch : null;
  }

  /**
   * Calculate match score (0-1)
   */
  private calculateMatchScore(matchedText: string, normalizedName: string, fullText: string): number {
    let score = 0;

    // Base score from matched text length vs name length
    const cleanedMatch = this.cleanName(matchedText).toLowerCase();
    const nameLength = normalizedName.length;
    const matchLength = cleanedMatch.length;

    score = Math.min(matchLength / nameLength, 1) * 0.6;

    // Bonus for match in title (first 100 chars)
    const titlePart = fullText.substring(0, 100);
    if (titlePart.includes(cleanedMatch)) {
      score += 0.2;
    }

    // Bonus for "жк" or "жилой комплекс" prefix near match
    const prefixPattern = /(?:жк|жилой комплекс|житловий комплекс)\s*$/i;
    const matchIndex = fullText.indexOf(cleanedMatch);
    const beforeMatch = fullText.substring(Math.max(0, matchIndex - 30), matchIndex);
    if (prefixPattern.test(beforeMatch)) {
      score += 0.2;
    }

    return Math.min(score, 1);
  }

  /**
   * Find complex by coordinates (point in polygon or nearest)
   */
  async findComplexByCoordinates(lat: number, lng: number): Promise<ApartmentComplex | null> {
    const inPolygon = await this.complexRepository.query(`
      SELECT id, name_ru, name_uk, lat, lng, geo_id, street_id,
             polygon IS NOT NULL as has_polygon
      FROM apartment_complexes
      WHERE polygon IS NOT NULL
        AND ST_Contains(polygon, ST_SetSRID(ST_MakePoint($1, $2), 4326))
      LIMIT 1
    `, [lng, lat]);

    if (inPolygon.length > 0) {
      return this.mapRowToComplex(inPolygon[0]);
    }

    const nearest = await this.complexRepository.query(`
      SELECT id, name_ru, name_uk, lat, lng, geo_id, street_id,
             polygon IS NOT NULL as has_polygon,
             ST_Distance(
               ST_SetSRID(ST_MakePoint(lng, lat), 4326)::geography,
               ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography
             ) as distance
      FROM apartment_complexes
      WHERE ST_DWithin(
        ST_SetSRID(ST_MakePoint(lng, lat), 4326)::geography,
        ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography,
        100
      )
      ORDER BY distance
      LIMIT 1
    `, [lng, lat]);

    if (nearest.length > 0) {
      return this.mapRowToComplex(nearest[0]);
    }

    return null;
  }

  /**
   * Main method: find complex by text first, then by coordinates
   */
  async findComplex(
    title: string,
    description?: string,
    lat?: number,
    lng?: number,
  ): Promise<{ complex: ApartmentComplex | null; method: 'text' | 'coordinates' | null }> {
    // 1. Try text matching first (more reliable for OLX)
    const textMatch = this.findComplexInText(title, description);
    if (textMatch && textMatch.score >= 0.6) {
      const complex = this.complexes.find(c => c.id === textMatch.complexId);
      if (complex) {
        return { complex, method: 'text' };
      }
    }

    // 2. Try coordinates if available
    if (lat && lng && !isNaN(lat) && !isNaN(lng)) {
      const coordMatch = await this.findComplexByCoordinates(lat, lng);
      if (coordMatch) {
        return { complex: coordMatch, method: 'coordinates' };
      }
    }

    // 3. Use text match even with lower score
    if (textMatch && textMatch.score >= 0.5) {
      const complex = this.complexes.find(c => c.id === textMatch.complexId);
      if (complex) {
        return { complex, method: 'text' };
      }
    }

    return { complex: null, method: null };
  }

  private mapRowToComplex(row: any): ApartmentComplex {
    return {
      id: row.id,
      nameRu: row.name_ru,
      nameUk: row.name_uk,
      nameNormalized: row.name_normalized || '',
      lat: parseFloat(row.lat),
      lng: parseFloat(row.lng),
      hasPolygon: row.has_polygon,
      geoId: row.geo_id,
      streetId: row.street_id,
    };
  }

  /**
   * Get complex by ID
   */
  getComplexById(id: number): ApartmentComplex | undefined {
    return this.complexes.find(c => c.id === id);
  }

  /**
   * Search complexes by name (for API)
   */
  searchByName(query: string, limit = 10): ApartmentComplex[] {
    const normalized = this.cleanName(query).toLowerCase();
    if (normalized.length < 2) return [];

    return this.complexes
      .filter(c =>
        c.nameNormalized.includes(normalized) ||
        c.nameRu.toLowerCase().includes(normalized) ||
        c.nameUk.toLowerCase().includes(normalized)
      )
      .slice(0, limit);
  }
}
