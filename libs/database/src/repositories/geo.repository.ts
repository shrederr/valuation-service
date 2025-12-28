import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, EntityManager, MoreThanOrEqual, IsNull } from 'typeorm';
import { GeoType, WGS84_SRID } from '@libs/common';

import { Geo } from '../entities/geo.entity';

@Injectable()
export class GeoRepository {
  public constructor(
    @InjectRepository(Geo)
    private readonly repository: Repository<Geo>,
  ) {}

  public get manager(): EntityManager {
    return this.repository.manager;
  }

  public createQueryBuilder(alias?: string) {
    return this.repository.createQueryBuilder(alias);
  }

  public async findOne(options: Parameters<Repository<Geo>['findOne']>[0]): Promise<Geo | null> {
    return this.repository.findOne(options);
  }

  public async find(options?: Parameters<Repository<Geo>['find']>[0]): Promise<Geo[]> {
    return this.repository.find(options);
  }

  public async save(entity: Geo | Partial<Geo>): Promise<Geo> {
    return this.repository.save(entity as Geo);
  }

  public async findById(id: number): Promise<Geo | null> {
    return this.repository.findOne({ where: { id } });
  }

  public async findByOsmId(osmId: string): Promise<Geo | null> {
    return this.repository.findOne({ where: { osmId } });
  }

  public async getChildren(parentId: number): Promise<Geo[]> {
    const parent = await this.findOne({ where: { id: parentId } });

    if (!parent || parent.lft === null || parent.rgt === null) {
      return [];
    }

    return this.createQueryBuilder('geo')
      .where('geo.lft > :lft', { lft: parent.lft })
      .andWhere('geo.rgt < :rgt', { rgt: parent.rgt })
      .orderBy('geo.lft')
      .getMany();
  }

  public async getDirectChildren(parentId: number): Promise<Geo[]> {
    return this.find({ where: { parentId } });
  }

  public async getAncestors(nodeId: number): Promise<Geo[]> {
    const node = await this.findOne({ where: { id: nodeId } });

    if (!node || node.lft === null || node.rgt === null) {
      return [];
    }

    return this.createQueryBuilder('geo')
      .where('geo.lft < :lft', { lft: node.lft })
      .andWhere('geo.rgt > :rgt', { rgt: node.rgt })
      .orderBy('geo.lft')
      .getMany();
  }

  public async rebuildNestedSet(): Promise<void> {
    await this.manager.transaction(async (transactionalEntityManager) => {
      const roots = await transactionalEntityManager.find(Geo, {
        where: { parentId: IsNull() },
        order: { id: 'ASC' },
      });

      let counter = 1;

      for (const root of roots) {
        counter = await this.rebuildNode(transactionalEntityManager, root.id, counter, 1);
      }
    });
  }

  private async rebuildNode(manager: EntityManager, nodeId: number, counter: number, level: number): Promise<number> {
    const lft = counter;
    let currentCounter = counter + 1;

    const children = await manager.find(Geo, {
      where: { parentId: nodeId },
      order: { id: 'ASC' },
    });

    for (const child of children) {
      currentCounter = await this.rebuildNode(manager, child.id, currentCounter, level + 1);
    }

    const rgt = currentCounter;
    currentCounter++;

    await manager.update(Geo, { id: nodeId }, { lft, rgt, lvl: level });

    return currentCounter;
  }

  public async shiftRlValues(manager: EntityManager, from: number, delta: number): Promise<void> {
    await Promise.all([
      manager.update(Geo, { lft: MoreThanOrEqual(from) }, { lft: () => `lft + ${delta}` }),
      manager.update(Geo, { rgt: MoreThanOrEqual(from) }, { rgt: () => `rgt + ${delta}` }),
    ]);
  }

  public async findNearbyByNameAndType(
    type: GeoType,
    nameUk: string,
    lng: number,
    lat: number,
    distanceMeters: number = 1000,
  ): Promise<Geo | null> {
    const result = await this.createQueryBuilder('g')
      .where('g.type = :type', { type })
      .andWhere("g.name->>'uk' = :nameUk", { nameUk })
      .andWhere('g.lat IS NOT NULL')
      .andWhere('g.lng IS NOT NULL')
      .andWhere(
        `ST_DWithin(
          ST_SetSRID(ST_Point(g.lng, g.lat), ${WGS84_SRID})::geography,
          ST_SetSRID(ST_Point(:lng, :lat), ${WGS84_SRID})::geography,
          :distance
        )`,
        { lng, lat, distance: distanceMeters },
      )
      .getOne();

    return result;
  }

  public async findGeoByPoint(lng: number, lat: number): Promise<Geo | null> {
    const result = await this.createQueryBuilder('g')
      .where('g.polygon IS NOT NULL')
      .andWhere(`ST_Contains(g.polygon, ST_SetSRID(ST_Point(:lng, :lat), ${WGS84_SRID}))`, { lng, lat })
      .orderBy('ST_Area(g.polygon)', 'ASC')
      .getOne();

    return result;
  }

  public async findParentByPoint(lng: number, lat: number): Promise<number | null> {
    const result = await this.findGeoByPoint(lng, lat);
    return result?.id ?? null;
  }

  public async findAllGeosByPoint(lng: number, lat: number): Promise<Geo[]> {
    return this.createQueryBuilder('g')
      .where('g.polygon IS NOT NULL')
      .andWhere(`ST_Contains(g.polygon, ST_SetSRID(ST_Point(:lng, :lat), ${WGS84_SRID}))`, { lng, lat })
      .orderBy('ST_Area(g.polygon)', 'ASC')
      .getMany();
  }

  public async findCityParentByPolygon(polygonWkt: string): Promise<number | null> {
    const result = await this.createQueryBuilder('g')
      .select('g.id')
      .where("g.type = 'city'")
      .andWhere('g.polygon IS NOT NULL')
      .andWhere(`ST_Contains(g.polygon, ST_Centroid(ST_GeomFromText(:polygonWkt, ${WGS84_SRID})))`, { polygonWkt })
      .orderBy('ST_Area(g.polygon)', 'ASC')
      .getOne();

    return result?.id ?? null;
  }

  public async findParentByPolygonCentroid(polygonWkt: string): Promise<number | null> {
    const result = await this.createQueryBuilder('g')
      .select('g.id')
      .where('g.polygon IS NOT NULL')
      .andWhere(`ST_Contains(g.polygon, ST_Centroid(ST_GeomFromText(:polygonWkt, ${WGS84_SRID})))`, { polygonWkt })
      .orderBy('ST_Area(g.polygon)', 'ASC')
      .getOne();

    return result?.id ?? null;
  }

  public async updatePolygonFromWkt(id: number, polygonWkt: string): Promise<void> {
    await this.createQueryBuilder()
      .update(Geo)
      .set({ polygon: () => `ST_Multi(ST_GeomFromText('${polygonWkt.replace(/'/g, "''")}', ${WGS84_SRID}))` })
      .where('id = :id', { id })
      .execute();
  }

  public async generateBufferPolygon(id: number, lng: number, lat: number, radiusMeters: number): Promise<void> {
    await this.createQueryBuilder()
      .update(Geo)
      .set({
        polygon: () => `ST_Multi(
          ST_Transform(
            ST_Buffer(
              ST_Transform(ST_SetSRID(ST_Point(${lng}, ${lat}), ${WGS84_SRID}), 3857),
              ${radiusMeters}
            ),
            ${WGS84_SRID}
          )
        )`,
      })
      .where('id = :id', { id })
      .execute();
  }

  public async getNextId(): Promise<number> {
    const result = await this.createQueryBuilder('g').select('MAX(g.id)', 'maxId').getRawOne();
    return (result?.maxId ?? 0) + 1;
  }

  public async getMaxRgt(): Promise<number> {
    const result = await this.createQueryBuilder('g').select('MAX(g.rgt)', 'maxRgt').getRawOne();
    return result?.maxRgt ?? 0;
  }

  public async findGeoByPointAndTypes(lng: number, lat: number, types: GeoType[]): Promise<Geo | null> {
    if (types.length === 0) return null;

    return this.createQueryBuilder('g')
      .where('g.polygon IS NOT NULL')
      .andWhere('g.type IN (:...types)', { types })
      .andWhere(`ST_Contains(g.polygon, ST_SetSRID(ST_Point(:lng, :lat), ${WGS84_SRID}))`, { lng, lat })
      .orderBy('ST_Area(g.polygon)', 'ASC')
      .getOne();
  }

  public async findNearestGeoByTypes(lng: number, lat: number, types: GeoType[], maxDistanceMeters: number = 10000): Promise<Geo | null> {
    if (types.length === 0) return null;

    // Priority order: city_district > city > village
    // First try to find city or city_district within 5km
    const cityTypes = types.filter(t => t === GeoType.City || t === GeoType.CityDistrict);
    if (cityTypes.length > 0) {
      const cityGeo = await this.createQueryBuilder('g')
        .where('g.polygon IS NOT NULL')
        .andWhere('g.type IN (:...cityTypes)', { cityTypes })
        .andWhere(
          `ST_DWithin(
            g.polygon::geography,
            ST_SetSRID(ST_Point(:lng, :lat), ${WGS84_SRID})::geography,
            :distance
          )`,
          { lng, lat, distance: 5000 },
        )
        .orderBy(`ST_Distance(g.polygon::geography, ST_SetSRID(ST_Point(:lng, :lat), ${WGS84_SRID})::geography)`, 'ASC')
        .setParameter('lng', lng)
        .setParameter('lat', lat)
        .getOne();

      if (cityGeo) return cityGeo;
    }

    // Fallback to any type within maxDistance
    return this.createQueryBuilder('g')
      .where('g.polygon IS NOT NULL')
      .andWhere('g.type IN (:...types)', { types })
      .andWhere(
        `ST_DWithin(
          g.polygon::geography,
          ST_SetSRID(ST_Point(:lng, :lat), ${WGS84_SRID})::geography,
          :distance
        )`,
        { lng, lat, distance: maxDistanceMeters },
      )
      .orderBy(`ST_Distance(g.polygon::geography, ST_SetSRID(ST_Point(:lng, :lat), ${WGS84_SRID})::geography)`, 'ASC')
      .setParameter('lng', lng)
      .setParameter('lat', lat)
      .getOne();
  }
}
