import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { lastValueFrom } from 'rxjs';
import { ConfigService } from '@nestjs/config';
import { GeoType, DEFAULT_OVERPASS_URL, WGS84_SRID } from '@libs/common';
import { GeoRepository, StreetRepository, Geo, Street } from '@libs/database';

type OverpassElement = {
  type: 'node' | 'way';
  id: number;
  lat?: number;
  lon?: number;
  nodes?: number[];
  tags?: Record<string, string>;
};

type OverpassResponse = {
  elements: OverpassElement[];
};

type OverpassWay = {
  id: number;
  nodes: number[];
  tags: Record<string, string>;
};

type SettlementInfo = {
  id: number;
  name: string;
  type: GeoType;
};

type StreetParseResult = {
  streetsCount: number;
  errorCount: number;
};

type RegionParseResult = {
  regionId: number;
  regionName: string;
  settlementsProcessed: number;
  settlementsFailed: number;
  totalStreets: number;
  totalErrors: number;
};

@Injectable()
export class StreetOsmParserService {
  private readonly logger = new Logger(StreetOsmParserService.name);

  public constructor(
    private readonly geoRepository: GeoRepository,
    private readonly streetRepository: StreetRepository,
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
  ) {}

  private getOverpassUrl(): string {
    return this.configService.get<string>('OVERPASS_URL') || DEFAULT_OVERPASS_URL;
  }

  public async getAvailableRegions(): Promise<Array<{ id: number; name: string; alias: string }>> {
    const regions = await this.geoRepository
      .createQueryBuilder('geo')
      .select('geo.id', 'id')
      .addSelect("geo.name->>'uk'", 'name')
      .addSelect('geo.alias', 'alias')
      .where('geo.type = :type', { type: GeoType.Region })
      .orderBy("geo.name->>'uk'")
      .getRawMany<{ id: number; name: string; alias: string }>();

    return regions || [];
  }

  public async findRegionByAlias(alias: string): Promise<{ id: number; name: string } | null> {
    const region = await this.geoRepository
      .createQueryBuilder('geo')
      .select('geo.id', 'id')
      .addSelect("geo.name->>'uk'", 'name')
      .where('geo.type = :type', { type: GeoType.Region })
      .andWhere('geo.alias = :alias', { alias })
      .getRawOne<{ id: number; name: string }>();

    return region || null;
  }

  public async getSettlementsInRegion(regionId: number): Promise<SettlementInfo[]> {
    const settlements = await this.geoRepository
      .createQueryBuilder('child')
      .innerJoin(Geo, 'parent', 'child.lft > parent.lft AND child.rgt < parent.rgt')
      .select('child.id', 'id')
      .addSelect("child.name->>'uk'", 'name')
      .addSelect('child.type', 'type')
      .where('parent.id = :regionId', { regionId })
      .andWhere('child.type IN (:...types)', { types: [GeoType.City, GeoType.Village] })
      .andWhere('child.polygon IS NOT NULL')
      .orderBy('child.type', 'DESC')
      .addOrderBy("child.name->>'uk'")
      .getRawMany<SettlementInfo>();

    return settlements || [];
  }

  public async parseSettlementStreets(geoId: number): Promise<StreetParseResult> {
    const result = await this.geoRepository
      .createQueryBuilder('geo')
      .select('geo.id', 'id')
      .addSelect('geo.name', 'name')
      .addSelect('geo.type', 'type')
      .addSelect('(geo.polygon IS NOT NULL)', 'has_polygon')
      .where('geo.id = :geoId', { geoId })
      .getRawOne<{ id: number; name: { uk?: string; ru?: string; en?: string }; type: GeoType; has_polygon: boolean }>();

    if (!result) {
      throw new Error(`Settlement ${geoId} not found`);
    }

    if (!result.has_polygon) {
      throw new Error(`Settlement ${geoId} has no polygon`);
    }

    const settlementName = result.name?.uk || result.name?.ru || result.name?.en;
    this.logger.log(`Parsing streets for: ${settlementName} (ID: ${geoId}, type: ${result.type})`);

    const polyCoords = await this.getPolygonCoords(geoId);
    const response = await this.queryOverpassApi(polyCoords);

    const { nodeCoordinates, ways } = this.parseOverpassResponse(response);
    const streetGroups = this.groupWaysByStreetName(ways);

    this.logger.log(`Found ${ways.length} ways, grouped into ${streetGroups.size} streets`);

    let processedCount = 0;
    let errorCount = 0;

    for (const [streetName, streetWays] of streetGroups.entries()) {
      processedCount++;

      try {
        await this.processStreetGroup(geoId, streetName, streetWays, nodeCoordinates);
      } catch (error) {
        errorCount++;
        this.logger.error(`Error processing "${streetName}": ${(error as Error).message}`);
      }

      if (processedCount % 100 === 0) {
        this.logger.log(`Progress: ${processedCount}/${streetGroups.size}`);
      }
    }

    this.logger.log(`Completed: ${settlementName} - ${processedCount} streets, ${errorCount} errors`);

    return { streetsCount: processedCount, errorCount };
  }

  public async parseRegionStreets(regionId: number): Promise<RegionParseResult> {
    const regionResult = await this.geoRepository
      .createQueryBuilder('geo')
      .select('geo.name', 'name')
      .where('geo.id = :regionId', { regionId })
      .getRawOne<{ name: { uk?: string; ru?: string; en?: string } }>();

    const regionName = regionResult?.name?.uk || `Region ${regionId}`;

    this.logger.log(`\n${'='.repeat(60)}`);
    this.logger.log(`Starting region: ${regionName} (ID: ${regionId})`);
    this.logger.log('='.repeat(60));

    let totalStreets = 0;
    let totalErrors = 0;
    let settlementsProcessed = 0;
    let settlementsFailed = 0;

    const settlements = await this.getSettlementsInRegion(regionId);
    this.logger.log(`Found ${settlements.length} settlements in region`);

    for (const settlement of settlements) {
      try {
        this.logger.log(`\nProcessing: ${settlement.name} (${settlement.type})`);
        const result = await this.parseSettlementStreets(settlement.id);
        totalStreets += result.streetsCount;
        totalErrors += result.errorCount;
        settlementsProcessed++;
      } catch (error) {
        settlementsFailed++;
        this.logger.error(`Failed to parse ${settlement.name} (${settlement.id}): ${(error as Error).message}`);
      }

      await new Promise((resolve) => setTimeout(resolve, 2000));
    }

    this.logger.log(`\nRegion completed: ${regionName}`);
    this.logger.log(`Settlements: ${settlementsProcessed} processed, ${settlementsFailed} failed`);
    this.logger.log(`Streets: ${totalStreets} total, ${totalErrors} errors`);

    return {
      regionId,
      regionName,
      settlementsProcessed,
      settlementsFailed,
      totalStreets,
      totalErrors,
    };
  }

  private async getPolygonCoords(geoId: number): Promise<string> {
    const polyResult = await this.geoRepository
      .createQueryBuilder('geo')
      .select('ST_AsText(ST_ExteriorRing((ST_Dump(geo.polygon)).geom))', 'coords')
      .where('geo.id = :geoId', { geoId })
      .getRawOne<{ coords: string }>();

    if (!polyResult?.coords) {
      throw new Error(`Could not get polygon for geo ${geoId}`);
    }

    const coordsMatch = polyResult.coords.match(/LINESTRING\((.*)\)/);

    if (!coordsMatch) {
      throw new Error(`Invalid polygon format for geo ${geoId}`);
    }

    return coordsMatch[1]
      .split(',')
      .map((pair) => {
        const [lon, lat] = pair.trim().split(' ');

        return `${lat} ${lon}`;
      })
      .join(' ');
  }

  private async queryOverpassApi(polyCoords: string): Promise<OverpassResponse> {
    const query = `[out:json][timeout:300];(way["highway"]["name"](poly:"${polyCoords}"););(._;>;);out body;`;

    this.logger.log('Querying Overpass API...');

    const httpResponse = await lastValueFrom(
      this.httpService.post(this.getOverpassUrl(), `data=${encodeURIComponent(query)}`, {
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        timeout: 310000,
      }),
    );

    return httpResponse.data;
  }

  private parseOverpassResponse(response: OverpassResponse): {
    nodeCoordinates: Map<number, { lat: number; lon: number }>;
    ways: OverpassWay[];
  } {
    const nodeCoordinates = new Map<number, { lat: number; lon: number }>();
    const ways: OverpassWay[] = [];

    response.elements.forEach((el: OverpassElement) => {
      if (el.type === 'node' && el.lat !== undefined && el.lon !== undefined) {
        nodeCoordinates.set(el.id, { lat: el.lat, lon: el.lon });
      } else if (el.type === 'way' && el.nodes && el.tags) {
        ways.push({ id: el.id, nodes: el.nodes, tags: el.tags });
      }
    });

    return { nodeCoordinates, ways };
  }

  private groupWaysByStreetName(ways: OverpassWay[]): Map<string, OverpassWay[]> {
    const streetGroups = new Map<string, OverpassWay[]>();

    ways.forEach((way) => {
      const primaryName = way.tags.name || way.tags['name:uk'];

      if (!primaryName) return;

      if (!streetGroups.has(primaryName)) {
        streetGroups.set(primaryName, []);
      }

      streetGroups.get(primaryName)!.push(way);
    });

    return streetGroups;
  }

  private buildCoordinatesFromNodes(nodes: number[], nodeCoordinates: Map<number, { lat: number; lon: number }>): string[] {
    return nodes
      .map((nodeId) => nodeCoordinates.get(nodeId))
      .filter((coord): coord is { lat: number; lon: number } => coord !== undefined)
      .map((coord) => `${coord.lon} ${coord.lat}`);
  }

  private async processStreetGroup(
    geoId: number,
    streetName: string,
    ways: OverpassWay[],
    nodeCoordinates: Map<number, { lat: number; lon: number }>,
  ): Promise<void> {
    const osmIds: number[] = [];
    const lineSegments: string[][] = [];

    ways.forEach((way) => {
      osmIds.push(way.id);
      const coordinates = this.buildCoordinatesFromNodes(way.nodes, nodeCoordinates);

      if (coordinates.length > 1) {
        lineSegments.push(coordinates);
      }
    });

    if (!lineSegments.length) return;

    const { name, names } = this.extractNamesFromWays(ways, streetName);
    const geometryWkt = this.buildGeometryWkt(lineSegments);
    const alias = this.generateAlias(name.uk);

    const existing = await this.streetRepository.findByNameInGeo(geoId, name.uk);

    if (existing) {
      existing.osmId = osmIds[0].toString();
      existing.name = name;
      existing.names = names;
      existing.alias = alias;
      await this.streetRepository.save(existing);
      await this.streetRepository.updateLineFromWkt(existing.id, geometryWkt);
    } else {
      const nextId = await this.streetRepository.getNextId();

      const street: Partial<Street> = {
        id: nextId,
        geoId,
        osmId: osmIds[0].toString(),
        name,
        names,
        alias,
      };

      const saved = await this.streetRepository.save(street);
      await this.streetRepository.updateLineFromWkt(saved.id, geometryWkt);
    }
  }

  private extractNamesFromWays(
    ways: OverpassWay[],
    streetName: string,
  ): { name: { uk: string; ru: string; en: string }; names: Record<string, string> } {
    const nameVariants = { uk: new Set<string>(), ru: new Set<string>(), en: new Set<string>() };

    ways.forEach((way) => {
      this.addTagValues(way.tags['name:uk'], nameVariants.uk);
      this.addTagValues(way.tags.name, nameVariants.uk);
      this.addTagValues(way.tags['name:ru'], nameVariants.ru);
      this.addTagValues(way.tags['name:en'], nameVariants.en);
      this.addTagValues(way.tags['alt_name:uk'], nameVariants.uk);
      this.addTagValues(way.tags['alt_name:ru'], nameVariants.ru);
      this.addTagValues(way.tags['alt_name:en'], nameVariants.en);
      this.addTagValues(way.tags.alt_name, nameVariants.uk);
      this.addTagValues(way.tags.old_name, nameVariants.uk);
      this.addTagValues(way.tags['old_name:uk'], nameVariants.uk);
      this.addTagValues(way.tags['old_name:ru'], nameVariants.ru);
      this.addTagValues(way.tags['old_name:en'], nameVariants.en);
    });

    return {
      name: {
        uk: Array.from(nameVariants.uk)[0] || streetName,
        ru: Array.from(nameVariants.ru)[0] || '',
        en: Array.from(nameVariants.en)[0] || '',
      },
      names: {
        uk: Array.from(nameVariants.uk).join('; '),
        ru: Array.from(nameVariants.ru).join('; '),
        en: Array.from(nameVariants.en).join('; '),
      },
    };
  }

  private addTagValues(value: string | undefined, set: Set<string>): void {
    if (!value) return;

    value.split(';').forEach((v) => {
      const trimmed = v.trim();

      if (trimmed) set.add(trimmed);
    });
  }

  private buildGeometryWkt(lineSegments: string[][]): string {
    if (lineSegments.length === 1) {
      return `LINESTRING(${lineSegments[0].join(', ')})`;
    }

    const lineStrings = lineSegments.map((segment) => `(${segment.join(', ')})`).join(', ');

    return `MULTILINESTRING(${lineStrings})`;
  }

  private generateAlias(name: string): string {
    const translitMap: Record<string, string> = {
      а: 'a',
      б: 'b',
      в: 'v',
      г: 'h',
      ґ: 'g',
      д: 'd',
      е: 'e',
      є: 'ye',
      ж: 'zh',
      з: 'z',
      и: 'y',
      і: 'i',
      ї: 'yi',
      й: 'y',
      к: 'k',
      л: 'l',
      м: 'm',
      н: 'n',
      о: 'o',
      п: 'p',
      р: 'r',
      с: 's',
      т: 't',
      у: 'u',
      ф: 'f',
      х: 'kh',
      ц: 'ts',
      ч: 'ch',
      ш: 'sh',
      щ: 'shch',
      ь: '',
      ю: 'yu',
      я: 'ya',
      "'": '',
      ʼ: '',
    };

    return name
      .toLowerCase()
      .split('')
      .map((char) => translitMap[char] || char)
      .join('')
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-|-$/g, '')
      .substring(0, 100);
  }
}
