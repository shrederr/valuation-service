import { Injectable, Logger, OnModuleInit, Inject, forwardRef } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { Geo, Street, Topzone, ApartmentComplex, UnifiedListing } from '@libs/database';
import { GeoType, MultiLanguageDto, SourceType, DealType, RealtyType } from '@libs/common';
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

@Injectable()
export class InitialSyncService implements OnModuleInit {
  private readonly logger = new Logger(InitialSyncService.name);
  private readonly vectorApiUrl: string;
  private readonly aggregatorApiUrl: string;
  private readonly syncOnStartup: boolean;
  private readonly skipInitialSync: boolean;
  private readonly batchSize: number;
  private isSyncing = false;

  constructor(
    private readonly configService: ConfigService,
    private readonly consumerControl: ConsumerControlService,
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
    this.vectorApiUrl = this.configService.getOrThrow<string>('VECTOR_API_URL');
    this.aggregatorApiUrl = this.configService.getOrThrow<string>('AGGREGATOR_API_URL');
    this.syncOnStartup = this.configService.get<string>('SYNC_ON_STARTUP') === 'true';
    this.skipInitialSync = this.configService.get<string>('SKIP_INITIAL_SYNC') === 'true';
    this.batchSize = this.configService.get<number>('SYNC_BATCH_SIZE') || 100;
  }

  /**
   * Called when module initializes.
   * Checks if database is empty and runs initial sync if needed.
   */
  async onModuleInit(): Promise<void> {
    // Skip for local development
    if (this.skipInitialSync) {
      this.logger.log('Initial sync skipped (SKIP_INITIAL_SYNC=true)');
      return;
    }

    if (!this.syncOnStartup) {
      this.logger.log('Initial sync on startup is disabled');
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

  /**
   * Check if the database is empty (no geo records).
   */
  async isDatabaseEmpty(): Promise<boolean> {
    const count = await this.geoRepository.count();
    return count === 0;
  }

  /**
   * Run full initial sync from external APIs.
   * Pauses consumers during sync, then resumes them after.
   */
  async runFullSync(): Promise<void> {
    if (this.isSyncing) {
      this.logger.warn('Sync already in progress, skipping');
      return;
    }

    this.isSyncing = true;
    const startTime = Date.now();
    this.logger.log('Starting full initial sync...');

    // Pause consumers - messages will queue in RabbitMQ
    await this.consumerControl.pauseConsumers();

    try {
      // Sync geo data first (order matters due to relations)
      await this.syncGeo();
      await this.syncStreets();
      await this.syncTopzones();
      await this.syncComplexes();

      // Sync properties
      await this.syncVectorProperties();
      await this.syncAggregatorProperties();

      const duration = ((Date.now() - startTime) / 1000).toFixed(2);
      this.logger.log(`Full initial sync completed in ${duration}s`);
    } catch (error) {
      this.logger.error('Initial sync failed', error instanceof Error ? error.stack : undefined);
      throw error;
    } finally {
      // Always resume consumers, even on error
      await this.consumerControl.resumeConsumers();
      this.isSyncing = false;
    }
  }

  // === Geo Sync ===

  async syncGeo(): Promise<void> {
    this.logger.log('Syncing geo data...');
    let page = 1;
    let totalSynced = 0;

    while (true) {
      const response = await this.fetchFromVector<VectorGeoDto>('geo/list', { page, perPage: this.batchSize });

      if (!response.items || response.items.length === 0) {
        break;
      }

      for (const item of response.items) {
        await this.upsertGeo(item);
        totalSynced++;
      }

      if (response.items.length < this.batchSize) {
        break;
      }

      page++;
    }

    this.logger.log(`Geo sync completed: ${totalSynced} records`);
  }

  private async upsertGeo(data: VectorGeoDto): Promise<void> {
    const existing = await this.geoRepository.findOne({ where: { id: data.id } });

    const geoData = {
      id: data.id,
      name: data.name,
      alias: data.alias,
      type: data.type as GeoType,
      lvl: data.lvl,
      lft: data.lft ?? 0,
      rgt: data.rgt ?? 0,
      lat: this.extractNumber(data.lat),
      lng: this.extractNumber(data.lng),
      bounds: data.bounds,
      declension: data.declension,
      syncedAt: new Date(),
    };

    if (existing) {
      await this.geoRepository.update(data.id, geoData);
    } else {
      await this.geoRepository.save(this.geoRepository.create(geoData));
    }
  }

  // === Streets Sync ===

  async syncStreets(): Promise<void> {
    this.logger.log('Syncing streets...');
    let page = 1;
    let totalSynced = 0;

    while (true) {
      const response = await this.fetchFromVector<VectorStreetDto>('geo/street', { page, perPage: this.batchSize });

      if (!response.items || response.items.length === 0) {
        break;
      }

      for (const item of response.items) {
        await this.upsertStreet(item);
        totalSynced++;
      }

      if (response.items.length < this.batchSize) {
        break;
      }

      page++;
    }

    this.logger.log(`Streets sync completed: ${totalSynced} records`);
  }

  private async upsertStreet(data: VectorStreetDto): Promise<void> {
    const existing = await this.streetRepository.findOne({ where: { id: data.id } });

    const streetData = {
      id: data.id,
      name: data.name,
      alias: data.alias,
      geoId: data.geoId,
      bounds: data.bounds,
      coordinates: data.coordinates,
      syncedAt: new Date(),
    };

    if (existing) {
      await this.streetRepository.update(data.id, streetData);
    } else {
      await this.streetRepository.save(this.streetRepository.create(streetData));
    }
  }

  // === Topzones Sync ===

  async syncTopzones(): Promise<void> {
    this.logger.log('Syncing topzones...');
    let page = 1;
    let totalSynced = 0;

    while (true) {
      const response = await this.fetchFromVector<VectorTopzoneDto>('geo/topzone', { page, perPage: this.batchSize });

      if (!response.items || response.items.length === 0) {
        break;
      }

      for (const item of response.items) {
        await this.upsertTopzone(item);
        totalSynced++;
      }

      if (response.items.length < this.batchSize) {
        break;
      }

      page++;
    }

    this.logger.log(`Topzones sync completed: ${totalSynced} records`);
  }

  private async upsertTopzone(data: VectorTopzoneDto): Promise<void> {
    const existing = await this.topzoneRepository.findOne({ where: { id: data.id } });

    const topzoneData = {
      id: data.id,
      name: data.name,
      alias: data.alias,
      lat: this.extractNumber(data.lat),
      lng: this.extractNumber(data.lng),
      bounds: data.bounds,
      declension: data.declension,
      coordinates: data.coordinates,
      syncedAt: new Date(),
    };

    if (existing) {
      await this.topzoneRepository.update(data.id, topzoneData);
    } else {
      await this.topzoneRepository.save(this.topzoneRepository.create(topzoneData));
    }
  }

  // === Complexes Sync ===

  async syncComplexes(): Promise<void> {
    this.logger.log('Syncing apartment complexes...');
    let page = 1;
    let totalSynced = 0;

    while (true) {
      const response = await this.fetchFromVector<VectorComplexDto>('apartment-complexes/list', { page, perPage: this.batchSize });

      if (!response.items || response.items.length === 0) {
        break;
      }

      for (const item of response.items) {
        await this.upsertComplex(item);
        totalSynced++;
      }

      if (response.items.length < this.batchSize) {
        break;
      }

      page++;
    }

    this.logger.log(`Complexes sync completed: ${totalSynced} records`);
  }

  private async upsertComplex(data: VectorComplexDto): Promise<void> {
    const existing = await this.complexRepository.findOne({ where: { id: data.id } });

    const name: MultiLanguageDto = typeof data.name === 'string' ? { uk: data.name } : data.name;

    const complexData = {
      id: data.id,
      name,
      geoId: data.geoId,
      topzoneId: data.topzoneId,
      lat: this.extractNumber(data.lat),
      lng: this.extractNumber(data.lng),
      type: data.type,
      syncedAt: new Date(),
    };

    if (existing) {
      await this.complexRepository.update(data.id, complexData);
    } else {
      await this.complexRepository.save(this.complexRepository.create(complexData));
    }
  }

  // === Vector Properties Sync ===

  async syncVectorProperties(): Promise<void> {
    this.logger.log('Syncing vector properties...');
    let page = 1;
    let totalSynced = 0;

    while (true) {
      const response = await this.fetchFromVector<VectorPropertyDto>('properties/list', { page, perPage: this.batchSize });

      if (!response.items || response.items.length === 0) {
        break;
      }

      for (const item of response.items) {
        await this.upsertVectorProperty(item);
        totalSynced++;
      }

      if (response.items.length < this.batchSize) {
        break;
      }

      page++;
    }

    this.logger.log(`Vector properties sync completed: ${totalSynced} records`);
  }

  private async upsertVectorProperty(data: VectorPropertyDto): Promise<void> {
    const existing = await this.listingRepository.findOne({
      where: { sourceType: SourceType.VECTOR, sourceId: data.id },
    });

    const listingData = {
      sourceType: SourceType.VECTOR,
      sourceId: data.id,
      sourceGlobalId: data.globalId,
      dealType: this.mapDealType(data.dealType),
      realtyType: this.mapRealtyType(data.realtyType),
      realtySubtype: data.realtySubtype,
      geoId: data.geoId,
      streetId: data.streetId,
      topzoneId: data.topzoneId,
      complexId: data.complexId,
      houseNumber: data.houseNumber,
      apartmentNumber: data.apartmentNumber ? parseInt(data.apartmentNumber, 10) : undefined,
      corps: data.corps,
      lat: this.extractNumber(data.lat),
      lng: this.extractNumber(data.lng),
      price: this.extractNumber(data.attributes?.price),
      currency: (data.attributes?.currency as string) || 'USD',
      pricePerMeter: this.extractNumber(data.attributes?.price_sqr ?? data.attributes?.pricePerMeter),
      totalArea: this.extractNumber(data.attributes?.square_total ?? data.attributes?.totalArea),
      livingArea: this.extractNumber(data.attributes?.square_living ?? data.attributes?.livingArea),
      kitchenArea: this.extractNumber(data.attributes?.square_kitchen ?? data.attributes?.kitchenArea),
      rooms: this.extractInteger(data.attributes?.rooms_count ?? data.attributes?.rooms),
      floor: this.extractInteger(data.attributes?.floor),
      totalFloors: this.extractInteger(data.attributes?.floors_count ?? data.attributes?.totalFloors),
      condition: data.attributes?.condition as string,
      houseType: data.attributes?.houseType as string,
      attributes: data.attributes,
      isActive: !data.isArchived,
      syncedAt: new Date(),
    };

    if (existing) {
      const merged = this.listingRepository.merge(existing, listingData);
      await this.listingRepository.save(merged);
    } else {
      await this.listingRepository.save(this.listingRepository.create(listingData));
    }
  }

  // === Aggregator Properties Sync ===

  async syncAggregatorProperties(): Promise<void> {
    this.logger.log('Syncing aggregator properties...');
    let page = 1;
    let totalSynced = 0;

    while (true) {
      const response = await this.fetchFromAggregator<AggregatorPropertyDto>('properties/list', { page, perPage: this.batchSize });

      if (!response.items || response.items.length === 0) {
        break;
      }

      for (const item of response.items) {
        await this.upsertAggregatorProperty(item);
        totalSynced++;
      }

      if (response.items.length < this.batchSize) {
        break;
      }

      page++;
    }

    this.logger.log(`Aggregator properties sync completed: ${totalSynced} records`);
  }

  private async upsertAggregatorProperty(data: AggregatorPropertyDto): Promise<void> {
    const existing = await this.listingRepository.findOne({
      where: { sourceType: SourceType.AGGREGATOR, sourceId: data.id },
    });

    // Validate geo references - aggregator may use different IDs
    // Set to null if reference doesn't exist in our database
    let validGeoId: number | undefined = undefined;
    let validStreetId: number | undefined = undefined;
    let validTopzoneId: number | undefined = undefined;
    let validComplexId: number | undefined = undefined;

    if (data.geoId) {
      const geoExists = await this.geoRepository.findOne({ where: { id: data.geoId }, select: ['id'] });
      validGeoId = geoExists ? data.geoId : undefined;
    }
    if (data.streetId) {
      const streetExists = await this.streetRepository.findOne({ where: { id: data.streetId }, select: ['id'] });
      validStreetId = streetExists ? data.streetId : undefined;
    }
    if (data.topzoneId) {
      const topzoneExists = await this.topzoneRepository.findOne({ where: { id: data.topzoneId }, select: ['id'] });
      validTopzoneId = topzoneExists ? data.topzoneId : undefined;
    }
    if (data.complexId) {
      const complexExists = await this.complexRepository.findOne({ where: { id: data.complexId }, select: ['id'] });
      validComplexId = complexExists ? data.complexId : undefined;
    }

    const listingData = {
      sourceType: SourceType.AGGREGATOR,
      sourceId: data.id,
      dealType: this.mapDealType(data.dealType),
      realtyType: this.mapRealtyType(data.realtyType),
      geoId: validGeoId,
      streetId: validStreetId,
      topzoneId: validTopzoneId,
      complexId: validComplexId,
      houseNumber: data.houseNumber,
      lat: this.extractNumber(data.lat),
      lng: this.extractNumber(data.lng),
      price: data.price,
      currency: data.currency || 'USD',
      pricePerMeter: this.extractNumber(data.attributes?.price_sqr ?? data.attributes?.pricePerMeter),
      totalArea: this.extractNumber(data.attributes?.square_total ?? data.attributes?.totalArea),
      livingArea: this.extractNumber(data.attributes?.square_living ?? data.attributes?.livingArea),
      kitchenArea: this.extractNumber(data.attributes?.square_kitchen ?? data.attributes?.kitchenArea),
      rooms: this.extractInteger(data.attributes?.rooms_count ?? data.attributes?.rooms),
      floor: this.extractInteger(data.attributes?.floor),
      totalFloors: this.extractInteger(data.attributes?.floors_count ?? data.attributes?.totalFloors),
      condition: data.attributes?.condition as string,
      houseType: data.attributes?.houseType as string,
      attributes: data.attributes,
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

  // === Helper Methods ===

  private async fetchFromVector<T>(endpoint: string, params: Record<string, unknown>): Promise<PaginatedResponseDto<T>> {
    const url = new URL(endpoint, this.vectorApiUrl);
    Object.entries(params).forEach(([key, value]) => {
      url.searchParams.set(key, String(value));
    });

    try {
      const response = await fetch(url.toString(), {
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${this.configService.get<string>('VECTOR_API_TOKEN')}`,
        },
      });

      if (!response.ok) {
        throw new Error(`Vector API request failed: ${response.status} ${response.statusText}`);
      }

      const data = await response.json();

      // Handle different response formats
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
      this.logger.error(`Failed to fetch from Vector API: ${endpoint}`, error instanceof Error ? error.message : undefined);
      return { items: [], total: 0, page: 1, pageSize: this.batchSize };
    }
  }

  private async fetchFromAggregator<T>(endpoint: string, params: Record<string, unknown>): Promise<PaginatedResponseDto<T>> {
    const url = new URL(endpoint, this.aggregatorApiUrl);
    Object.entries(params).forEach(([key, value]) => {
      url.searchParams.set(key, String(value));
    });

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

      // Handle different response formats
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
