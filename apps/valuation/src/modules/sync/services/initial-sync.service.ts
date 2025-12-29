import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { Geo, Street, Topzone, ApartmentComplex, UnifiedListing } from '@libs/database';
import { GeoType, MultiLanguageDto, SourceType, DealType, RealtyType, AttributeMapperService } from '@libs/common';
import {
  PaginatedResponseDto,
  VectorGeoDto,
  VectorStreetDto,
  VectorTopzoneDto,
  VectorComplexDto,
  VectorPropertyDto,
  AggregatorPropertyDto,
} from '../dto/initial-sync.dto';
import { ConsumerControlService } from './consumer-control.service';
import { GeoLookupService } from '../../osm/geo-lookup.service';

@Injectable()
export class InitialSyncService implements OnModuleInit {
  private readonly logger = new Logger(InitialSyncService.name);
  private readonly vectorApiUrl: string;
  private readonly aggregatorApiUrl: string;
  private readonly syncOnStartup: boolean;
  private readonly skipInitialSync: boolean;
  private readonly forceInitialSync: boolean;
  private readonly batchSize: number;
  private isSyncing = false;

  constructor(
    private readonly configService: ConfigService,
    private readonly consumerControl: ConsumerControlService,
    private readonly geoLookupService: GeoLookupService,
    private readonly attributeMapperService: AttributeMapperService,
    @InjectRepository(Geo)
    private readonly geoRepository: Repository<Geo>,
    @InjectRepository(Street)
    private readonly streetRepository: Repository<Street>,
    @InjectRepository(Topzone)
    private readonly topzoneRepository: Repository<Topzone>,
    @InjectRepository(ApartmentComplex)
    private readonly complexRepository: Repository<ApartmentComplex>,
    @InjectRepository(UnifiedListing)
    private readonly listingRepository: Repository<UnifiedListing>,
  ) {
    this.vectorApiUrl = this.configService.get<string>('VECTOR_API_URL') || '';
    this.aggregatorApiUrl = this.configService.get<string>('AGGREGATOR_API_URL') || '';
    this.syncOnStartup = this.configService.get<string>('SYNC_ON_STARTUP') === 'true';
    this.skipInitialSync = this.configService.get<string>('SKIP_INITIAL_SYNC') === 'true';
    this.forceInitialSync = this.configService.get<string>('FORCE_INITIAL_SYNC') === 'true';
    this.batchSize = this.configService.get<number>('SYNC_BATCH_SIZE') || 100;
  }

  async onModuleInit(): Promise<void> {
    if (this.skipInitialSync) {
      this.logger.log('Initial sync skipped (SKIP_INITIAL_SYNC=true)');
      return;
    }
    if (!this.syncOnStartup) {
      this.logger.log('Initial sync on startup is disabled');
      return;
    }
    if (this.forceInitialSync) {
      this.logger.log('FORCE_INITIAL_SYNC=true, running sync regardless of database state...');
      await this.runFullSync();
      return;
    }
    const isEmpty = await this.isDatabaseEmpty();
    if (!isEmpty) {
      this.logger.log('Database is not empty, skipping initial sync');
      return;
    }
    this.logger.log('Database is empty, starting initial sync...');
    await this.runFullSync();
  }

  async isDatabaseEmpty(): Promise<boolean> {
    const count = await this.listingRepository.count();
    return count === 0;
  }

  async runFullSync(): Promise<void> {
    if (this.isSyncing) {
      this.logger.warn('Sync already in progress, skipping');
      return;
    }
    this.isSyncing = true;
    const startTime = Date.now();
    this.logger.log('Starting full initial sync...');
    await this.consumerControl.pauseConsumers();
    try {
      await this.syncAggregatorProperties();
      const duration = ((Date.now() - startTime) / 1000).toFixed(2);
      this.logger.log(`Initial sync completed in ${duration}s`);
    } catch (error) {
      this.logger.error('Initial sync failed', error instanceof Error ? error.stack : undefined);
      throw error;
    } finally {
      await this.consumerControl.resumeConsumers();
      this.isSyncing = false;
    }
  }

  async syncAggregatorProperties(): Promise<void> {
    this.logger.log('Syncing aggregator properties with GeoLookupService...');
    let page = 1;
    let totalSynced = 0;
    while (true) {
      const response = await this.fetchFromAggregator<AggregatorPropertyDto>('properties/list', { page, perPage: this.batchSize });
      if (!response.items || response.items.length === 0) break;
      for (const item of response.items) {
        await this.upsertAggregatorProperty(item);
        totalSynced++;
      }
      if (response.items.length < this.batchSize) break;
      page++;
    }
    this.logger.log(`Aggregator properties sync completed: ${totalSynced} records`);
  }

  private async upsertAggregatorProperty(data: AggregatorPropertyDto): Promise<void> {
    const existing = await this.listingRepository.findOne({
      where: { sourceType: SourceType.AGGREGATOR, sourceId: data.id },
    });

    const platform = this.attributeMapperService.detectPlatform(data.realtyPlatform, data.url);

    let resolvedGeoId: number | undefined;
    let resolvedStreetId: number | undefined;
    let resolvedComplexId: number | undefined;
    let resolvedTopzoneId: number | undefined;

    const lng = this.extractNumber(data.lng);
    const lat = this.extractNumber(data.lat);

    if (lng && lat) {
      const textForMatching = this.buildTextForMatching(data);
      try {
        const geoResolution = await this.geoLookupService.resolveGeoForListingWithText(
          lng, lat, textForMatching, data.geoId,
        );
        if (geoResolution) {
          resolvedGeoId = geoResolution.geoId || undefined;
          resolvedStreetId = geoResolution.streetId || undefined;
          
          
        }
      } catch (error) {
        this.logger.warn(`Geo lookup failed for property ${data.id}: ${error instanceof Error ? error.message : 'Unknown'}`);
      }
    }

    if (!resolvedGeoId && data.geoId) {
      const g = await this.geoRepository.findOne({ where: { id: data.geoId }, select: ['id'] });
      resolvedGeoId = g ? data.geoId : undefined;
    }
    if (!resolvedTopzoneId && data.topzoneId) {
      const t = await this.topzoneRepository.findOne({ where: { id: data.topzoneId }, select: ['id'] });
      resolvedTopzoneId = t ? data.topzoneId : undefined;
    }
    if (!resolvedComplexId && data.complexId) {
      const c = await this.complexRepository.findOne({ where: { id: data.complexId }, select: ['id'] });
      resolvedComplexId = c ? data.complexId : undefined;
    }

    const attrResult = this.attributeMapperService.mapAttributes(
      platform,
      data.attributes as Record<string, unknown> | undefined,
      data.primaryData as Record<string, unknown> | undefined,
    );

    const listingData = {
      sourceType: SourceType.AGGREGATOR,
      sourceId: data.id,
      dealType: this.mapDealType(data.dealType),
      realtyType: this.mapRealtyType(data.realtyType),
      geoId: resolvedGeoId,
      streetId: resolvedStreetId,
      topzoneId: resolvedTopzoneId,
      complexId: resolvedComplexId,
      houseNumber: data.houseNumber,
      lat,
      lng,
      price: data.price,
      currency: data.currency || 'USD',
      pricePerMeter: this.extractNumber(data.attributes?.price_sqr ?? data.attributes?.pricePerMeter),
      totalArea: this.extractNumber(data.attributes?.square_total ?? data.attributes?.totalArea),
      livingArea: this.extractNumber(data.attributes?.square_living ?? data.attributes?.livingArea),
      kitchenArea: this.extractNumber(data.attributes?.square_kitchen ?? data.attributes?.kitchenArea),
      rooms: this.extractInteger(data.attributes?.rooms_count ?? data.attributes?.rooms),
      floor: this.extractInteger(data.attributes?.floor),
      totalFloors: this.extractInteger(data.attributes?.floors_count ?? data.attributes?.totalFloors),
      condition: attrResult.condition,
      houseType: attrResult.houseType,
      attributes: data.attributes,
      primaryData: data.primaryData as Record<string, unknown>,
      realtyPlatform: platform,
      description: data.description as unknown as MultiLanguageDto,
      externalUrl: data.url,
      isActive: data.isActive,
      syncedAt: new Date(),
    };

    if (existing) {
      const merged = this.listingRepository.merge(existing, listingData);
      await this.listingRepository.save(merged);
    } else {
      await this.listingRepository.save(this.listingRepository.create(listingData));
    }
  }

  private buildTextForMatching(data: AggregatorPropertyDto): string {
    const parts: string[] = [];
    if (data.description) {
      if (typeof data.description === 'string') {
        parts.push(data.description);
      } else if (typeof data.description === 'object') {
        const desc = data.description as { uk?: string; ru?: string };
        if (desc.uk) parts.push(desc.uk);
        if (desc.ru) parts.push(desc.ru);
      }
    }
    const primaryData = data.primaryData as Record<string, unknown> | undefined;
    if (primaryData?.address && typeof primaryData.address === 'string') {
      parts.push(primaryData.address);
    }
    if (primaryData?.street_name && typeof primaryData.street_name === 'string') {
      parts.push(primaryData.street_name);
    }
    if (data.houseNumber) {
      parts.push(data.houseNumber);
    }
    return parts.join(' ');
  }

  private async fetchFromAggregator<T>(endpoint: string, params: Record<string, unknown>): Promise<PaginatedResponseDto<T>> {
    const url = new URL(endpoint, this.aggregatorApiUrl);
    Object.entries(params).forEach(([key, value]) => url.searchParams.set(key, String(value)));
    try {
      const response = await fetch(url.toString(), {
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${this.configService.get<string>('AGGREGATOR_API_TOKEN')}`,
        },
      });
      if (!response.ok) {
        throw new Error(`Aggregator API request failed: ${response.status} ${response.statusText}`);
      }
      const data = await response.json();
      if (Array.isArray(data)) {
        return { items: data, total: data.length, page: 1, pageSize: data.length };
      }
      return {
        items: data.items || data.data || [],
        total: data.total || data.count || 0,
        page: data.page || 1,
        pageSize: data.pageSize || data.limit || this.batchSize,
      };
    } catch (error) {
      this.logger.error(`Failed to fetch from Aggregator API: ${endpoint}`, error instanceof Error ? error.message : undefined);
      return { items: [], total: 0, page: 1, pageSize: this.batchSize };
    }
  }

  private mapDealType(dealType: string): DealType {
    const normalized = dealType?.toLowerCase();
    if (normalized === 'rent') return DealType.Rent;
    if (normalized === 'sell' || normalized === 'buy' || normalized === 'sale') return DealType.Sell;
    return DealType.Sell;
  }

  private mapRealtyType(realtyType: string): RealtyType {
    const normalized = realtyType?.toLowerCase();
    if (normalized === 'apartment' || normalized === 'flat') return RealtyType.Apartment;
    if (normalized === 'house') return RealtyType.House;
    if (normalized === 'commercial') return RealtyType.Commercial;
    if (normalized === 'land' || normalized === 'area') return RealtyType.Area;
    if (normalized === 'garage') return RealtyType.Garage;
    if (normalized === 'room') return RealtyType.Room;
    return RealtyType.Apartment;
  }

  private extractNumber(value: unknown): number | undefined {
    if (value === null || value === undefined) return undefined;
    if (typeof value === 'number') return isNaN(value) ? undefined : value;
    if (typeof value === 'string') {
      const num = parseFloat(value);
      return isNaN(num) ? undefined : num;
    }
    return undefined;
  }

  private extractInteger(value: unknown): number | undefined {
    const num = this.extractNumber(value);
    if (num === undefined) return undefined;
    return Math.floor(num);
  }
}
