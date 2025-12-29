import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { UnifiedListing, Geo } from '@libs/database';

export interface SearchLevel {
  name: string;
  priority: number;
  query: (subject: UnifiedListing) => Promise<UnifiedListing[]>;
}

/**
 * Стратегия географического поиска аналогов с приоритетами:
 *
 * 1. Тот же дом (улица + номер дома) или тот же ЖК
 * 2. Тот же квартал (радиус ~200м по координатам)
 * 3. Та же улица
 * 4. Та же топзона (микрорайон)
 * 5. Тот же район
 * 6. Соседние районы
 * 7. Весь город
 *
 * Логика: собираем аналоги поэтапно, пока не наберем нужное количество (10)
 */
@Injectable()
export class GeoFallbackStrategy {
  private readonly logger = new Logger(GeoFallbackStrategy.name);

  // Радиус для поиска "в квартале" в метрах
  private readonly BLOCK_RADIUS_METERS = 200;

  public constructor(
    @InjectRepository(UnifiedListing)
    private readonly listingRepository: Repository<UnifiedListing>,
    @InjectRepository(Geo)
    private readonly geoRepository: Repository<Geo>,
  ) {}

  public getSearchLevels(subject: UnifiedListing): SearchLevel[] {
    return [
      {
        name: 'building',
        priority: 1,
        query: (s) => this.searchInBuilding(s),
      },
      {
        name: 'block',
        priority: 2,
        query: (s) => this.searchInBlock(s),
      },
      {
        name: 'street',
        priority: 3,
        query: (s) => this.searchOnStreet(s),
      },
      {
        name: 'topzone',
        priority: 4,
        query: (s) => this.searchInTopzone(s),
      },
      {
        name: 'district',
        priority: 5,
        query: (s) => this.searchInDistrict(s),
      },
      {
        name: 'neighbor_districts',
        priority: 6,
        query: (s) => this.searchInNeighborDistricts(s),
      },
      {
        name: 'city',
        priority: 7,
        query: (s) => this.searchInCity(s),
      },
    ];
  }

  /**
   * Приоритет 1: Поиск в том же доме
   * - По ЖК (complexId) если есть
   * - Или по улице + номеру дома
   */
  private async searchInBuilding(subject: UnifiedListing): Promise<UnifiedListing[]> {
    const baseConditions = this.getBaseConditions(subject);

    // Сначала ищем по ЖК
    if (subject.complexId) {
      const results = await this.listingRepository
        .createQueryBuilder('l')
        .leftJoinAndSelect('l.geo', 'geo')
        .leftJoinAndSelect('l.street', 'street')
        .where('l.complexId = :complexId', { complexId: subject.complexId })
        .andWhere('l.id != :id', { id: subject.id })
        .andWhere('l.isActive = true')
        .andWhere('l.dealType = :dealType', { dealType: subject.dealType })
        .andWhere('l.realtyType = :realtyType', { realtyType: subject.realtyType })
        .getMany();

      if (results.length > 0) {
        this.logger.debug(`Found ${results.length} analogs in same complex`);
        return results;
      }
    }

    // Если нет ЖК или не нашли - ищем по улице + дом
    if (subject.streetId && subject.houseNumber) {
      const results = await this.listingRepository
        .createQueryBuilder('l')
        .leftJoinAndSelect('l.geo', 'geo')
        .leftJoinAndSelect('l.street', 'street')
        .where('l.streetId = :streetId', { streetId: subject.streetId })
        .andWhere('l.houseNumber = :houseNumber', { houseNumber: subject.houseNumber })
        .andWhere('l.id != :id', { id: subject.id })
        .andWhere('l.isActive = true')
        .andWhere('l.dealType = :dealType', { dealType: subject.dealType })
        .andWhere('l.realtyType = :realtyType', { realtyType: subject.realtyType })
        .getMany();

      this.logger.debug(`Found ${results.length} analogs in same building (street + house)`);
      return results;
    }

    return [];
  }

  /**
   * Приоритет 2: Поиск в том же квартале (радиус ~200м)
   * Использует PostGIS для поиска по координатам
   */
  private async searchInBlock(subject: UnifiedListing): Promise<UnifiedListing[]> {
    if (!subject.lat || !subject.lng) {
      return [];
    }

    // Используем простую формулу для примерного расстояния
    // 1 градус широты ≈ 111 км, 1 градус долготы ≈ 111 км * cos(широта)
    const latDiff = this.BLOCK_RADIUS_METERS / 111000;
    const lngDiff = this.BLOCK_RADIUS_METERS / (111000 * Math.cos((subject.lat * Math.PI) / 180));

    const results = await this.listingRepository
      .createQueryBuilder('l')
      .leftJoinAndSelect('l.geo', 'geo')
      .leftJoinAndSelect('l.street', 'street')
      .where('l.lat BETWEEN :latMin AND :latMax', {
        latMin: subject.lat - latDiff,
        latMax: subject.lat + latDiff,
      })
      .andWhere('l.lng BETWEEN :lngMin AND :lngMax', {
        lngMin: subject.lng - lngDiff,
        lngMax: subject.lng + lngDiff,
      })
      .andWhere('l.id != :id', { id: subject.id })
      .andWhere('l.isActive = true')
      .andWhere('l.dealType = :dealType', { dealType: subject.dealType })
      .andWhere('l.realtyType = :realtyType', { realtyType: subject.realtyType })
      .getMany();

    this.logger.debug(`Found ${results.length} analogs within ${this.BLOCK_RADIUS_METERS}m radius`);
    return results;
  }

  /**
   * Приоритет 3: Поиск на той же улице
   */
  private async searchOnStreet(subject: UnifiedListing): Promise<UnifiedListing[]> {
    if (!subject.streetId) {
      return [];
    }

    const results = await this.listingRepository
      .createQueryBuilder('l')
      .leftJoinAndSelect('l.geo', 'geo')
      .leftJoinAndSelect('l.street', 'street')
      .where('l.streetId = :streetId', { streetId: subject.streetId })
      .andWhere('l.id != :id', { id: subject.id })
      .andWhere('l.isActive = true')
      .andWhere('l.dealType = :dealType', { dealType: subject.dealType })
      .andWhere('l.realtyType = :realtyType', { realtyType: subject.realtyType })
      .getMany();

    this.logger.debug(`Found ${results.length} analogs on same street`);
    return results;
  }

  /**
   * Приоритет 4: Поиск в той же топзоне (микрорайон)
   */
  private async searchInTopzone(subject: UnifiedListing): Promise<UnifiedListing[]> {
    if (!subject.topzoneId) {
      return [];
    }

    const results = await this.listingRepository
      .createQueryBuilder('l')
      .leftJoinAndSelect('l.geo', 'geo')
      .leftJoinAndSelect('l.street', 'street')
      .where('l.topzoneId = :topzoneId', { topzoneId: subject.topzoneId })
      .andWhere('l.id != :id', { id: subject.id })
      .andWhere('l.isActive = true')
      .andWhere('l.dealType = :dealType', { dealType: subject.dealType })
      .andWhere('l.realtyType = :realtyType', { realtyType: subject.realtyType })
      .getMany();

    this.logger.debug(`Found ${results.length} analogs in same topzone`);
    return results;
  }

  /**
   * Приоритет 5: Поиск в том же районе
   */
  private async searchInDistrict(subject: UnifiedListing): Promise<UnifiedListing[]> {
    if (!subject.geoId) {
      return [];
    }

    const results = await this.listingRepository
      .createQueryBuilder('l')
      .leftJoinAndSelect('l.geo', 'geo')
      .leftJoinAndSelect('l.street', 'street')
      .where('l.geoId = :geoId', { geoId: subject.geoId })
      .andWhere('l.id != :id', { id: subject.id })
      .andWhere('l.isActive = true')
      .andWhere('l.dealType = :dealType', { dealType: subject.dealType })
      .andWhere('l.realtyType = :realtyType', { realtyType: subject.realtyType })
      .getMany();

    this.logger.debug(`Found ${results.length} analogs in same district`);
    return results;
  }

  /**
   * Приоритет 6: Поиск в соседних районах
   */
  private async searchInNeighborDistricts(subject: UnifiedListing): Promise<UnifiedListing[]> {
    if (!subject.geoId) {
      return [];
    }

    const subjectGeo = await this.geoRepository.findOne({ where: { id: subject.geoId } });

    if (!subjectGeo || !subjectGeo.parentId) {
      return [];
    }

    const siblings = await this.geoRepository.find({ where: { parentId: subjectGeo.parentId } });
    const siblingIds = siblings.map((s) => s.id).filter((id) => id !== subject.geoId);

    if (siblingIds.length === 0) {
      return [];
    }

    const results = await this.listingRepository
      .createQueryBuilder('l')
      .leftJoinAndSelect('l.geo', 'geo')
      .leftJoinAndSelect('l.street', 'street')
      .where('l.geoId IN (:...geoIds)', { geoIds: siblingIds })
      .andWhere('l.id != :id', { id: subject.id })
      .andWhere('l.isActive = true')
      .andWhere('l.dealType = :dealType', { dealType: subject.dealType })
      .andWhere('l.realtyType = :realtyType', { realtyType: subject.realtyType })
      .getMany();

    this.logger.debug(`Found ${results.length} analogs in neighbor districts`);
    return results;
  }

  /**
   * Приоритет 7: Поиск во всем городе
   */
  private async searchInCity(subject: UnifiedListing): Promise<UnifiedListing[]> {
    if (!subject.geoId) {
      return [];
    }

    const subjectGeo = await this.geoRepository.findOne({ where: { id: subject.geoId } });

    if (!subjectGeo) {
      return [];
    }

    const cityGeo = await this.findCityAncestor(subjectGeo);

    if (!cityGeo) {
      return [];
    }

    const descendants = await this.geoRepository
      .createQueryBuilder('g')
      .where('g.lft >= :lft', { lft: cityGeo.lft })
      .andWhere('g.rgt <= :rgt', { rgt: cityGeo.rgt })
      .getMany();

    const descendantIds = descendants.map((d) => d.id);

    if (descendantIds.length === 0) {
      return [];
    }

    const results = await this.listingRepository
      .createQueryBuilder('l')
      .leftJoinAndSelect('l.geo', 'geo')
      .leftJoinAndSelect('l.street', 'street')
      .where('l.geoId IN (:...geoIds)', { geoIds: descendantIds })
      .andWhere('l.id != :id', { id: subject.id })
      .andWhere('l.isActive = true')
      .andWhere('l.dealType = :dealType', { dealType: subject.dealType })
      .andWhere('l.realtyType = :realtyType', { realtyType: subject.realtyType })
      .getMany();

    this.logger.debug(`Found ${results.length} analogs in city`);
    return results;
  }

  private async findCityAncestor(geo: Geo): Promise<Geo | null> {
    if (geo.type === 'city') {
      return geo;
    }

    const ancestors = await this.geoRepository
      .createQueryBuilder('g')
      .where('g.lft < :lft', { lft: geo.lft })
      .andWhere('g.rgt > :rgt', { rgt: geo.rgt })
      .orderBy('g.lft', 'DESC')
      .getMany();

    return ancestors.find((a) => a.type === 'city') || ancestors[0] || null;
  }

  private getBaseConditions(subject: UnifiedListing) {
    return {
      id: subject.id,
      dealType: subject.dealType,
      realtyType: subject.realtyType,
    };
  }
}
