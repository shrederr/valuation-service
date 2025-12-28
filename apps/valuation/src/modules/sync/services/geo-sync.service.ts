import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { Geo, Street, Topzone, ApartmentComplex, GeoTopzone } from '@libs/database';
import { GeoType, MultiLanguageDto } from '@libs/common';
import { GeoEventDto, StreetEventDto, TopzoneEventDto, ComplexEventDto } from '../dto';

@Injectable()
export class GeoSyncService {
  private readonly logger = new Logger(GeoSyncService.name);

  constructor(
    @InjectRepository(Geo)
    private readonly geoRepository: Repository<Geo>,
    @InjectRepository(Street)
    private readonly streetRepository: Repository<Street>,
    @InjectRepository(Topzone)
    private readonly topzoneRepository: Repository<Topzone>,
    @InjectRepository(ApartmentComplex)
    private readonly complexRepository: Repository<ApartmentComplex>,
    @InjectRepository(GeoTopzone)
    private readonly geoTopzoneRepository: Repository<GeoTopzone>,
  ) {}

  // === Geo Operations ===

  async handleGeoCreated(data: GeoEventDto): Promise<void> {
    try {
      const geo = this.geoRepository.create({
        id: data.id,
        name: data.name,
        alias: data.alias,
        type: data.type as GeoType,
        lvl: data.lvl,
        lft: data.lft,
        rgt: data.rgt,
        lat: data.lat,
        lng: data.lng,
        bounds: data.bounds,
        declension: data.declension,
        syncedAt: new Date(),
      });

      await this.geoRepository.save(geo);
      this.logger.log(`Geo created: ${data.id} - ${data.name?.uk || data.alias}`);
    } catch (error) {
      this.logger.error(`Failed to create geo ${data.id}`, error instanceof Error ? error.stack : undefined);
      throw error;
    }
  }

  async handleGeoUpdated(data: GeoEventDto): Promise<void> {
    try {
      await this.geoRepository.update(data.id, {
        name: data.name,
        alias: data.alias,
        type: data.type as GeoType,
        lvl: data.lvl,
        lft: data.lft,
        rgt: data.rgt,
        lat: data.lat,
        lng: data.lng,
        bounds: data.bounds,
        declension: data.declension,
        syncedAt: new Date(),
      });
      this.logger.log(`Geo updated: ${data.id} - ${data.name?.uk || data.alias}`);
    } catch (error) {
      this.logger.error(`Failed to update geo ${data.id}`, error instanceof Error ? error.stack : undefined);
      throw error;
    }
  }

  async handleGeoDeleted(id: number): Promise<void> {
    try {
      await this.geoRepository.delete(id);
      this.logger.log(`Geo deleted: ${id}`);
    } catch (error) {
      this.logger.error(`Failed to delete geo ${id}`, error instanceof Error ? error.stack : undefined);
      throw error;
    }
  }

  // === Street Operations ===

  async handleStreetCreated(data: StreetEventDto): Promise<void> {
    try {
      const street = this.streetRepository.create({
        id: data.id,
        name: data.name,
        alias: data.alias,
        geoId: data.geoId,
        bounds: data.bounds,
        coordinates: data.coordinates,
        syncedAt: new Date(),
      });

      await this.streetRepository.save(street);
      this.logger.log(`Street created: ${data.id} - ${data.name?.uk || data.alias}`);
    } catch (error) {
      this.logger.error(`Failed to create street ${data.id}`, error instanceof Error ? error.stack : undefined);
      throw error;
    }
  }

  async handleStreetUpdated(data: StreetEventDto): Promise<void> {
    try {
      await this.streetRepository.update(data.id, {
        name: data.name,
        alias: data.alias,
        geoId: data.geoId,
        bounds: data.bounds,
        coordinates: data.coordinates,
        syncedAt: new Date(),
      });
      this.logger.log(`Street updated: ${data.id} - ${data.name?.uk || data.alias}`);
    } catch (error) {
      this.logger.error(`Failed to update street ${data.id}`, error instanceof Error ? error.stack : undefined);
      throw error;
    }
  }

  async handleStreetDeleted(id: number): Promise<void> {
    try {
      await this.streetRepository.delete(id);
      this.logger.log(`Street deleted: ${id}`);
    } catch (error) {
      this.logger.error(`Failed to delete street ${id}`, error instanceof Error ? error.stack : undefined);
      throw error;
    }
  }

  // === Topzone Operations ===

  async handleTopzoneCreated(data: TopzoneEventDto): Promise<void> {
    try {
      const topzone = this.topzoneRepository.create({
        id: data.id,
        name: data.name,
        alias: data.alias,
        lat: data.lat,
        lng: data.lng,
        bounds: data.bounds,
        declension: data.declension,
        coordinates: data.coordinates,
        syncedAt: new Date(),
      });

      await this.topzoneRepository.save(topzone);
      this.logger.log(`Topzone created: ${data.id} - ${data.name?.uk || data.alias}`);
    } catch (error) {
      this.logger.error(`Failed to create topzone ${data.id}`, error instanceof Error ? error.stack : undefined);
      throw error;
    }
  }

  async handleTopzoneUpdated(data: TopzoneEventDto): Promise<void> {
    try {
      await this.topzoneRepository.update(data.id, {
        name: data.name,
        alias: data.alias,
        lat: data.lat,
        lng: data.lng,
        bounds: data.bounds,
        declension: data.declension,
        coordinates: data.coordinates,
        syncedAt: new Date(),
      });
      this.logger.log(`Topzone updated: ${data.id} - ${data.name?.uk || data.alias}`);
    } catch (error) {
      this.logger.error(`Failed to update topzone ${data.id}`, error instanceof Error ? error.stack : undefined);
      throw error;
    }
  }

  async handleTopzoneDeleted(id: number): Promise<void> {
    try {
      await this.topzoneRepository.delete(id);
      this.logger.log(`Topzone deleted: ${id}`);
    } catch (error) {
      this.logger.error(`Failed to delete topzone ${id}`, error instanceof Error ? error.stack : undefined);
      throw error;
    }
  }

  // === ApartmentComplex Operations ===

  async handleComplexCreated(data: ComplexEventDto): Promise<void> {
    try {
      const name: MultiLanguageDto = typeof data.name === 'string' ? { uk: data.name } : data.name;

      const complex = this.complexRepository.create({
        id: data.id,
        nameUk: name.uk || '',
        nameRu: name.ru || name.uk || '',
        nameEn: name.en,
        nameNormalized: (name.uk || '').toLowerCase().replace(/[^a-zа-яіїєґ0-9]/gi, ''),
        geoId: data.geoId,
        lat: data.lat,
        lng: data.lng,
        source: 'geovector',
      });

      await this.complexRepository.save(complex);
      const displayName = typeof data.name === 'string' ? data.name : data.name?.uk;
      this.logger.log(`Complex created: ${data.id} - ${displayName}`);
    } catch (error) {
      this.logger.error(`Failed to create complex ${data.id}`, error instanceof Error ? error.stack : undefined);
      throw error;
    }
  }

  async handleComplexUpdated(data: ComplexEventDto): Promise<void> {
    try {
      const name: MultiLanguageDto = typeof data.name === 'string' ? { uk: data.name } : data.name;

      await this.complexRepository.update(data.id, {
        nameUk: name.uk || '',
        nameRu: name.ru || name.uk || '',
        nameEn: name.en,
        nameNormalized: (name.uk || '').toLowerCase().replace(/[^a-zа-яіїєґ0-9]/gi, ''),
        geoId: data.geoId,
        lat: data.lat,
        lng: data.lng,
      });
      const displayName = typeof data.name === 'string' ? data.name : data.name?.uk;
      this.logger.log(`Complex updated: ${data.id} - ${displayName}`);
    } catch (error) {
      this.logger.error(`Failed to update complex ${data.id}`, error instanceof Error ? error.stack : undefined);
      throw error;
    }
  }

  async handleComplexDeleted(id: number): Promise<void> {
    try {
      await this.complexRepository.delete(id);
      this.logger.log(`Complex deleted: ${id}`);
    } catch (error) {
      this.logger.error(`Failed to delete complex ${id}`, error instanceof Error ? error.stack : undefined);
      throw error;
    }
  }
}
