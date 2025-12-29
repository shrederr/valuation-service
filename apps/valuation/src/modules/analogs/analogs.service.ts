import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { UnifiedListing, Geo } from '@libs/database';
import { SourceType, DealType, RealtyType } from '@libs/common';
import { AnalogDto, AnalogSearchResultDto } from '@libs/models';

import { GeoFallbackStrategy } from './strategies/geo-fallback.strategy';
import { AnalogFilterService } from './services/analog-filter.service';
import { AnalogScorerService } from './services/analog-scorer.service';

export interface AnalogSearchOptions {
  sourceType?: SourceType;
  sourceId?: number;
  listingId?: string;
  minAnalogs?: number;
  targetAnalogs?: number;
  maxAnalogs?: number;
}

const DEFAULT_MIN_ANALOGS = 5;
const DEFAULT_TARGET_ANALOGS = 10;
const DEFAULT_MAX_ANALOGS = 20;

@Injectable()
export class AnalogsService {
  private readonly logger = new Logger(AnalogsService.name);

  public constructor(
    @InjectRepository(UnifiedListing)
    private readonly listingRepository: Repository<UnifiedListing>,
    @InjectRepository(Geo)
    private readonly geoRepository: Repository<Geo>,
    private readonly geoFallbackStrategy: GeoFallbackStrategy,
    private readonly analogFilterService: AnalogFilterService,
    private readonly analogScorerService: AnalogScorerService,
  ) {}

  public async findAnalogs(options: AnalogSearchOptions): Promise<AnalogSearchResultDto> {
    const subject = await this.findSubjectListing(options);

    if (!subject) {
      throw new NotFoundException('Subject listing not found');
    }

    const minAnalogs = options.minAnalogs ?? DEFAULT_MIN_ANALOGS;
    const targetAnalogs = options.targetAnalogs ?? DEFAULT_TARGET_ANALOGS;
    const maxAnalogs = options.maxAnalogs ?? DEFAULT_MAX_ANALOGS;

    const { analogs, searchRadius } = await this.searchWithFallback(subject, minAnalogs, targetAnalogs, maxAnalogs);

    const scoredAnalogs = this.analogScorerService.scoreAnalogs(subject, analogs);
    const sortedAnalogs = scoredAnalogs.sort((a, b) => b.matchScore - a.matchScore);
    const limitedAnalogs = sortedAnalogs.slice(0, maxAnalogs);

    const result: AnalogSearchResultDto = {
      analogs: limitedAnalogs.map((a) => this.mapToAnalogDto(a.listing, a.matchScore)),
      totalCount: limitedAnalogs.length,
      searchRadius,
    };

    if (limitedAnalogs.length < minAnalogs) {
      result.warning = `Found only ${limitedAnalogs.length} analogs (minimum recommended: ${minAnalogs})`;
    }

    return result;
  }

  private async findSubjectListing(options: AnalogSearchOptions): Promise<UnifiedListing | null> {
    if (options.listingId) {
      return this.listingRepository.findOne({
        where: { id: options.listingId },
        relations: ['geo', 'street'],
      });
    }

    if (options.sourceType && options.sourceId) {
      return this.listingRepository.findOne({
        where: { sourceType: options.sourceType, sourceId: options.sourceId },
        relations: ['geo', 'street'],
      });
    }

    return null;
  }

  private async searchWithFallback(
    subject: UnifiedListing,
    minAnalogs: number,
    targetAnalogs: number,
    maxAnalogs: number,
  ): Promise<{ analogs: UnifiedListing[]; searchRadius: string }> {
    const searchLevels = this.geoFallbackStrategy.getSearchLevels(subject);
    const collectedAnalogs: UnifiedListing[] = [];
    let currentRadius = 'building';

    for (const level of searchLevels) {
      if (collectedAnalogs.length >= targetAnalogs) {
        break;
      }

      const candidates = await this.searchAtLevel(subject, level);
      const filtered = this.analogFilterService.filterCandidates(subject, candidates, collectedAnalogs);

      for (const candidate of filtered) {
        if (!collectedAnalogs.some((a) => a.id === candidate.id)) {
          collectedAnalogs.push(candidate);
        }
      }

      currentRadius = level.name;

      if (collectedAnalogs.length >= minAnalogs && level.name !== 'building') {
        break;
      }
    }

    return { analogs: collectedAnalogs, searchRadius: currentRadius };
  }

  private async searchAtLevel(
    subject: UnifiedListing,
    level: { name: string; query: (subject: UnifiedListing) => Promise<UnifiedListing[]> },
  ): Promise<UnifiedListing[]> {
    try {
      return await level.query(subject);
    } catch (error) {
      this.logger.warn(`Error searching at level ${level.name}: ${(error as Error).message}`);
      return [];
    }
  }

  private mapToAnalogDto(listing: UnifiedListing, matchScore: number): AnalogDto {
    const address = this.buildAddress(listing);

    return {
      id: listing.id,
      source: listing.sourceType,
      address,
      price: Number(listing.price) || 0,
      pricePerMeter: Number(listing.pricePerMeter) || 0,
      area: Number(listing.totalArea) || 0,
      rooms: listing.rooms,
      floor: listing.floor,
      totalFloors: listing.totalFloors,
      condition: listing.condition,
      houseType: listing.houseType,
      matchScore,
      externalUrl: listing.externalUrl,
    };
  }

  private buildAddress(listing: UnifiedListing): string {
    const parts: string[] = [];

    if (listing.street?.name) {
      const streetName = typeof listing.street.name === 'object' ? (listing.street.name as { uk?: string }).uk : listing.street.name;
      parts.push(streetName || '');
    }

    if (listing.houseNumber) {
      parts.push(listing.houseNumber);
    }

    if (listing.geo?.name) {
      const geoName = typeof listing.geo.name === 'object' ? (listing.geo.name as { uk?: string }).uk : listing.geo.name;
      parts.push(geoName || '');
    }

    return parts.filter(Boolean).join(', ');
  }
}
