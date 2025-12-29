import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, EntityManager } from 'typeorm';
import { WGS84_SRID } from '@libs/common';

import { Street } from '../entities/street.entity';

@Injectable()
export class StreetRepository {
  public constructor(
    @InjectRepository(Street)
    private readonly repository: Repository<Street>,
  ) {}

  public get manager(): EntityManager {
    return this.repository.manager;
  }

  public createQueryBuilder(alias?: string) {
    return this.repository.createQueryBuilder(alias);
  }

  public async findOne(options: Parameters<Repository<Street>['findOne']>[0]): Promise<Street | null> {
    return this.repository.findOne(options);
  }

  public async find(options?: Parameters<Repository<Street>['find']>[0]): Promise<Street[]> {
    return this.repository.find(options);
  }

  public async save(entity: Street | Partial<Street>): Promise<Street> {
    return this.repository.save(entity as Street);
  }

  public async findById(id: number): Promise<Street | null> {
    return this.repository.findOne({ where: { id } });
  }

  public async findByOsmId(osmId: string): Promise<Street | null> {
    return this.repository.findOne({ where: { osmId } });
  }

  public async findByGeoId(geoId: number): Promise<Street[]> {
    return this.repository.find({ where: { geoId } });
  }

  public async findByNameInGeo(geoId: number, nameUk: string): Promise<Street | null> {
    return this.createQueryBuilder('s')
      .where('s.geoId = :geoId', { geoId })
      .andWhere("s.name->>'uk' = :nameUk", { nameUk })
      .getOne();
  }

  public async searchByName(query: string, geoId?: number, limit: number = 20): Promise<Street[]> {
    const qb = this.createQueryBuilder('s')
      .where("s.name->>'uk' ILIKE :query", { query: `%${query}%` })
      .limit(limit);

    if (geoId) {
      qb.andWhere('s.geoId = :geoId', { geoId });
    }

    return qb.getMany();
  }

  public async searchByNameTrigram(query: string, geoId?: number, limit: number = 20): Promise<Street[]> {
    const qb = this.createQueryBuilder('s')
      .where("s.name->>'uk' % :query", { query })
      .orderBy(`similarity(s.name->>'uk', :query)`, 'DESC')
      .setParameter('query', query)
      .limit(limit);

    if (geoId) {
      qb.andWhere('s.geoId = :geoId', { geoId });
    }

    return qb.getMany();
  }

  public async findNearestStreet(lng: number, lat: number, geoId?: number, maxDistanceMeters: number = 500): Promise<Street | null> {
    const qb = this.createQueryBuilder('s')
      .where('s.line IS NOT NULL')
      .andWhere(
        `ST_DWithin(
          s.line::geography,
          ST_SetSRID(ST_Point(:lng, :lat), ${WGS84_SRID})::geography,
          :maxDistance
        )`,
        { lng, lat, maxDistance: maxDistanceMeters },
      )
      .orderBy(`ST_Distance(s.line::geography, ST_SetSRID(ST_Point(:lng, :lat), ${WGS84_SRID})::geography)`, 'ASC')
      .setParameter('lng', lng)
      .setParameter('lat', lat)
      .limit(1);

    if (geoId) {
      qb.andWhere('s.geoId = :geoId', { geoId });
    }

    return qb.getOne();
  }

  /**
   * Знайти N найближчих вулиць з відстанями
   */
  public async findNearestStreets(
    lng: number,
    lat: number,
    geoId?: number,
    limit: number = 5,
    maxDistanceMeters: number = 500,
  ): Promise<Array<{ street: Street; distanceMeters: number }>> {
    const qb = this.createQueryBuilder('s')
      .addSelect(
        `ST_Distance(s.line::geography, ST_SetSRID(ST_Point(:lng, :lat), ${WGS84_SRID})::geography)`,
        'distance_meters',
      )
      .where('s.line IS NOT NULL')
      .andWhere(
        `ST_DWithin(
          s.line::geography,
          ST_SetSRID(ST_Point(:lng, :lat), ${WGS84_SRID})::geography,
          :maxDistance
        )`,
        { lng, lat, maxDistance: maxDistanceMeters },
      )
      .orderBy('distance_meters', 'ASC')
      .setParameter('lng', lng)
      .setParameter('lat', lat)
      .limit(limit);

    if (geoId) {
      qb.andWhere('s.geoId = :geoId', { geoId });
    }

    const rawResults = await qb.getRawAndEntities();

    return rawResults.entities.map((street, index) => ({
      street,
      distanceMeters: parseFloat(rawResults.raw[index]?.distance_meters || '0'),
    }));
  }

  public async updateLineFromWkt(id: number, lineWkt: string): Promise<void> {
    await this.createQueryBuilder()
      .update(Street)
      .set({ line: () => `ST_Multi(ST_GeomFromText('${lineWkt.replace(/'/g, "''")}', ${WGS84_SRID}))` })
      .where('id = :id', { id })
      .execute();
  }

  public async getNextId(): Promise<number> {
    const result = await this.createQueryBuilder('s').select('MAX(s.id)', 'maxId').getRawOne();
    return (result?.maxId ?? 0) + 1;
  }
}
