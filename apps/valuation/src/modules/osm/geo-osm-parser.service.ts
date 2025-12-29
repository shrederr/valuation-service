import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { lastValueFrom } from 'rxjs';
import {
  GeoType,
  OsmRelation,
  OsmNodeWithTags,
  OsmWayWithTags,
  OverpassResponse,
  ParsedGeoData,
  NodeCoords,
  OSM_ADMIN_LEVEL_MAP,
  OSM_VILLAGE_PLACE_TAGS,
  OSM_BUFFER_RADIUS,
  DEFAULT_OVERPASS_URL,
} from '@libs/common';
import { GeoRepository, Geo } from '@libs/database';
import { ConfigService } from '@nestjs/config';

@Injectable()
export class GeoOsmParserService {
  private readonly logger = new Logger(GeoOsmParserService.name);

  public constructor(
    private readonly geoRepository: GeoRepository,
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
  ) {}

  public async parseRegion(regionOsmId: number, skipRebuildNestedSet = false): Promise<{ total: number; errors: number }> {
    this.logger.log(`Starting to parse region with OSM ID: ${regionOsmId}`);

    const regionData = await this.fetchRelationWithGeometry(regionOsmId);

    if (!regionData) {
      throw new Error(`Region with OSM ID ${regionOsmId} not found`);
    }

    const region = await this.saveGeo(regionData, null);
    this.logger.log(`Saved region: ${regionData.name.uk} (ID: ${region.id})`);

    const childrenData = await this.fetchChildrenInRegion(regionOsmId);
    this.logger.log(`Found ${childrenData.length} children in region`);

    let total = 1;
    let errors = 0;

    const byType = this.groupByType(childrenData);

    for (const districtData of byType.get(GeoType.RegionDistrict) || []) {
      try {
        await this.saveGeo(districtData, region.id);
        total++;
      } catch (error) {
        errors++;
        this.logger.error(`Error saving region district ${districtData.name.uk}: ${(error as Error).message}`);
      }
    }

    for (const cityData of byType.get(GeoType.City) || []) {
      try {
        const parentId = await this.findParentId(cityData, region.id);
        await this.saveGeo(cityData, parentId);
        total++;
      } catch (error) {
        errors++;
        this.logger.error(`Error saving city ${cityData.name.uk}: ${(error as Error).message}`);
      }
    }

    for (const villageData of byType.get(GeoType.Village) || []) {
      try {
        const parentId = await this.findParentId(villageData, region.id);
        await this.saveGeo(villageData, parentId);
        total++;
      } catch (error) {
        errors++;
        this.logger.error(`Error saving village ${villageData.name.uk}: ${(error as Error).message}`);
      }
    }

    for (const cityDistrictData of byType.get(GeoType.CityDistrict) || []) {
      try {
        const parentId = await this.findParentId(cityDistrictData, region.id);
        await this.saveGeo(cityDistrictData, parentId);
        total++;
      } catch (error) {
        errors++;
        this.logger.error(`Error saving city district ${cityDistrictData.name.uk}: ${(error as Error).message}`);
      }
    }

    if (!skipRebuildNestedSet) {
      this.logger.log('Rebuilding nested set...');
      await this.geoRepository.rebuildNestedSet();
    }

    this.logger.log(`Parsing completed. Total: ${total}, Errors: ${errors}`);

    return { total, errors };
  }

  public async rebuildNestedSet(): Promise<void> {
    this.logger.log('Rebuilding nested set for all regions...');
    await this.geoRepository.rebuildNestedSet();
    this.logger.log('Nested set rebuilt successfully');
  }

  private async fetchRelationWithGeometry(osmId: number): Promise<ParsedGeoData | null> {
    const query = `
      [out:json][timeout:300];
      relation(${osmId});
      out body;
      >;
      out skel qt;
    `;

    const response = await this.queryOverpass(query);
    const relation = response.elements.find((el): el is OsmRelation => el.type === 'relation' && el.id === osmId);

    if (!relation) {
      return null;
    }

    const geometry = this.buildPolygonFromElements(response.elements, relation);

    return this.parseRelation(relation, geometry);
  }

  private async fetchChildrenInRegion(regionOsmId: number): Promise<ParsedGeoData[]> {
    const query = `
      [out:json][timeout:600];
      relation(${regionOsmId});
      map_to_area->.region;
      (
        relation["boundary"="administrative"]["admin_level"~"^(6|9|10)$"](area.region);
        relation["place"~"^(city|town)$"](area.region);
        relation["place"~"^(village|hamlet)$"](area.region);
        way["place"~"^(city|town)$"](area.region);
        way["place"~"^(village|hamlet)$"](area.region);
        node["place"~"^(city|town)$"](area.region);
        node["place"~"^(village|hamlet)$"](area.region);
      );
      out body;
      >;
      out skel qt;
    `;

    const response = await this.queryOverpass(query);
    const results: ParsedGeoData[] = [];

    const relations = response.elements.filter((el): el is OsmRelation => el.type === 'relation');
    const nodeMap = new Map<number, NodeCoords>();
    const ways = new Map<number, number[]>();

    for (const el of response.elements) {
      if (el.type === 'node') {
        nodeMap.set(el.id, { lat: el.lat, lon: el.lon });
      } else if (el.type === 'way') {
        ways.set(el.id, el.nodes);
      }
    }

    for (const relation of relations) {
      try {
        const geometry = this.buildPolygonFromElements(response.elements, relation);
        const labelCoords = this.extractLabelCoords(relation, nodeMap, ways);
        const parsed = this.parseRelation(relation, geometry, labelCoords);

        if (parsed) {
          results.push(parsed);
        }
      } catch (error) {
        this.logger.warn(`Error parsing relation ${relation.id}: ${(error as Error).message}`);
      }
    }

    const placeWays = response.elements.filter(
      (el): el is OsmWayWithTags => el.type === 'way' && 'tags' in el && !!(el as OsmWayWithTags).tags?.place,
    );

    for (const way of placeWays) {
      try {
        const parsed = this.parsePlaceWay(way, nodeMap);

        if (parsed) {
          results.push(parsed);
        }
      } catch (error) {
        this.logger.warn(`Error parsing place way ${way.id}: ${(error as Error).message}`);
      }
    }

    const placeNodes = response.elements.filter((el): el is OsmNodeWithTags => el.type === 'node' && 'tags' in el && !!el.tags?.place);

    const existingPlaceKeys = new Set(
      results.filter((r) => r.type === GeoType.City || r.type === GeoType.Village).map((r) => `${r.name.uk?.toLowerCase()}:${r.type}`),
    );

    for (const node of placeNodes) {
      try {
        const parsed = this.parsePlaceNode(node);

        if (parsed) {
          const placeKey = `${parsed.name.uk?.toLowerCase()}:${parsed.type}`;

          if (existingPlaceKeys.has(placeKey)) {
            continue;
          }

          results.push(parsed);
        }
      } catch (error) {
        this.logger.warn(`Error parsing place node ${node.id}: ${(error as Error).message}`);
      }
    }

    return results;
  }

  private extractLabelCoords(
    relation: OsmRelation,
    nodeMap: Map<number, NodeCoords>,
    ways: Map<number, number[]>,
  ): { lat: number; lng: number } | undefined {
    if (!relation.members) {
      return undefined;
    }

    const labelNode = relation.members.find((m) => m.type === 'node' && (m.role === 'label' || m.role === 'admin_centre'));

    if (labelNode) {
      const coords = nodeMap.get(labelNode.ref);

      if (coords) {
        return { lat: coords.lat, lng: coords.lon };
      }
    }

    const firstWay = relation.members.find((m) => m.type === 'way' && (m.role === 'outer' || !m.role || m.role === ''));

    if (firstWay) {
      const wayNodes = ways.get(firstWay.ref);

      if (wayNodes && wayNodes.length > 0) {
        const firstNodeCoords = nodeMap.get(wayNodes[0]);

        if (firstNodeCoords) {
          return { lat: firstNodeCoords.lat, lng: firstNodeCoords.lon };
        }
      }
    }

    return undefined;
  }

  private parsePlaceWay(way: OsmWayWithTags, nodeMap: Map<number, NodeCoords>): ParsedGeoData | null {
    const tags = way.tags;

    if (!tags?.place) {
      return null;
    }

    const name = {
      uk: tags['name:uk'] || tags.name,
      ru: tags['name:ru'],
      en: tags['name:en'],
    };

    if (!name.uk) {
      return null;
    }

    const type = this.getPlaceType(tags.place);

    if (!type) {
      return null;
    }

    const coords: string[] = [];
    let firstCoord: NodeCoords | undefined;

    for (const nodeId of way.nodes) {
      const node = nodeMap.get(nodeId);

      if (node) {
        coords.push(`${node.lon} ${node.lat}`);

        if (!firstCoord) {
          firstCoord = node;
        }
      }
    }

    if (coords.length < 4) {
      return null;
    }

    if (coords[0] !== coords[coords.length - 1]) {
      coords.push(coords[0]);
    }

    return {
      osmId: way.id.toString(),
      name,
      type,
      adminLevel: 8,
      population: tags.population ? parseInt(tags.population, 10) : undefined,
      lat: firstCoord?.lat,
      lng: firstCoord?.lon,
      polygonWkt: `POLYGON((${coords.join(', ')}))`,
    };
  }

  private parsePlaceNode(node: OsmNodeWithTags): ParsedGeoData | null {
    const tags = node.tags;

    if (!tags?.place) {
      return null;
    }

    const name = {
      uk: tags['name:uk'] || tags.name,
      ru: tags['name:ru'],
      en: tags['name:en'],
    };

    if (!name.uk) {
      return null;
    }

    const type = this.getPlaceType(tags.place);

    if (!type) {
      return null;
    }

    return {
      osmId: node.id.toString(),
      name,
      type,
      adminLevel: 8,
      population: tags.population ? parseInt(tags.population, 10) : undefined,
      lat: node.lat,
      lng: node.lon,
    };
  }

  private parseRelation(relation: OsmRelation, geometryWkt?: string, labelCoords?: { lat: number; lng: number }): ParsedGeoData | null {
    const tags = relation.tags;
    const adminLevel = parseInt(tags.admin_level || '0', 10);

    let type: GeoType | null = null;

    if (tags.place === 'city' || tags.place === 'town') {
      type = GeoType.City;
    } else if (tags.place && OSM_VILLAGE_PLACE_TAGS.includes(tags.place)) {
      type = GeoType.Village;
    } else if (OSM_ADMIN_LEVEL_MAP[adminLevel]) {
      type = OSM_ADMIN_LEVEL_MAP[adminLevel];

      if (adminLevel === 8 && tags.place && OSM_VILLAGE_PLACE_TAGS.includes(tags.place)) {
        type = GeoType.Village;
      }
    }

    if (!type) {
      return null;
    }

    const name = {
      uk: tags['name:uk'] || tags.name,
      ru: tags['name:ru'],
      en: tags['name:en'],
    };

    if (!name.uk) {
      this.logger.warn(`Relation ${relation.id} has no Ukrainian name`);

      return null;
    }

    return {
      osmId: relation.id.toString(),
      name,
      type,
      adminLevel: adminLevel || 8,
      population: tags.population ? parseInt(tags.population, 10) : undefined,
      lat: labelCoords?.lat,
      lng: labelCoords?.lng,
      polygonWkt: geometryWkt,
    };
  }

  private getPlaceType(place: string): GeoType | null {
    if (place === 'city' || place === 'town') {
      return GeoType.City;
    }

    if (place === 'village' || place === 'hamlet') {
      return GeoType.Village;
    }

    return null;
  }

  private buildPolygonFromElements(elements: OverpassResponse['elements'], relation: OsmRelation): string | undefined {
    if (!relation.members) {
      return undefined;
    }

    const nodes = new Map<number, NodeCoords>();
    const ways = new Map<number, number[]>();

    for (const el of elements) {
      if (el.type === 'node') {
        nodes.set(el.id, { lat: el.lat, lon: el.lon });
      } else if (el.type === 'way') {
        ways.set(el.id, el.nodes);
      }
    }

    let outerWayRefs = relation.members.filter((m) => m.type === 'way' && m.role === 'outer').map((m) => m.ref);

    if (outerWayRefs.length === 0) {
      outerWayRefs = relation.members.filter((m) => m.type === 'way' && (!m.role || m.role === '')).map((m) => m.ref);
    }

    if (outerWayRefs.length === 0) {
      return undefined;
    }

    const foundWays = outerWayRefs.filter((ref) => ways.has(ref));

    if (foundWays.length === 0) {
      return undefined;
    }

    const mergedRings = this.mergeWaysIntoRings(outerWayRefs, ways, nodes);

    if (mergedRings.length === 0) {
      return undefined;
    }

    const closedRings = mergedRings
      .map((ring) => {
        if (ring.length < 4) {
          return null;
        }

        if (ring[0] !== ring[ring.length - 1]) {
          ring.push(ring[0]);
        }

        return ring;
      })
      .filter((ring): ring is string[] => ring !== null && ring.length >= 4);

    if (closedRings.length === 0) {
      return undefined;
    }

    if (closedRings.length === 1) {
      return `POLYGON((${closedRings[0].join(', ')}))`;
    }

    const polygons = closedRings.map((ring) => `((${ring.join(', ')}))`).join(', ');

    return `MULTIPOLYGON(${polygons})`;
  }

  private mergeWaysIntoRings(wayRefs: number[], ways: Map<number, number[]>, nodes: Map<number, NodeCoords>): string[][] {
    const unusedWays = new Set(wayRefs);
    const rings: string[][] = [];

    while (unusedWays.size > 0) {
      const firstWayRef = unusedWays.values().next().value;
      unusedWays.delete(firstWayRef);

      const wayNodes = ways.get(firstWayRef);

      if (!wayNodes || wayNodes.length === 0) {
        continue;
      }

      const nodeChain: number[] = [...wayNodes];
      let changed = true;

      while (changed && unusedWays.size > 0) {
        changed = false;
        const startNode = nodeChain[0];
        const endNode = nodeChain[nodeChain.length - 1];

        if (startNode === endNode) {
          break;
        }

        for (const wayRef of unusedWays) {
          const candidateNodes = ways.get(wayRef);

          if (!candidateNodes || candidateNodes.length === 0) {
            continue;
          }

          const candStart = candidateNodes[0];
          const candEnd = candidateNodes[candidateNodes.length - 1];

          if (candStart === endNode) {
            nodeChain.push(...candidateNodes.slice(1));
            unusedWays.delete(wayRef);
            changed = true;
            break;
          } else if (candEnd === endNode) {
            nodeChain.push(...candidateNodes.slice(0, -1).reverse());
            unusedWays.delete(wayRef);
            changed = true;
            break;
          } else if (candEnd === startNode) {
            nodeChain.unshift(...candidateNodes.slice(0, -1));
            unusedWays.delete(wayRef);
            changed = true;
            break;
          } else if (candStart === startNode) {
            nodeChain.unshift(...candidateNodes.slice(1).reverse());
            unusedWays.delete(wayRef);
            changed = true;
            break;
          }
        }
      }

      const coords: string[] = [];

      for (const nodeId of nodeChain) {
        const node = nodes.get(nodeId);

        if (node) {
          coords.push(`${node.lon} ${node.lat}`);
        }
      }

      if (coords.length >= 3) {
        rings.push(coords);
      }
    }

    return rings;
  }

  private async saveGeo(data: ParsedGeoData, parentId: number | null): Promise<Geo> {
    let existing = await this.geoRepository.findByOsmId(data.osmId);

    if (!existing && data.name.uk && data.lat && data.lng && !data.polygonWkt) {
      existing = await this.geoRepository.findNearbyByNameAndType(data.type, data.name.uk, data.lng, data.lat, 1000);
    }

    const nameUk = data.name.uk || '';
    const alias = this.generateAlias(nameUk);
    const geoName = { uk: nameUk, ru: data.name.ru, en: data.name.en };

    if (existing) {
      const existingIsNode = parseInt(existing.osmId || '0') > 100000000;
      const newIsRelation = parseInt(data.osmId) < 100000000;

      if (existingIsNode && newIsRelation) {
        existing.osmId = data.osmId;
      }

      existing.name = geoName;
      existing.type = data.type;
      existing.alias = alias;
      existing.parentId = parentId ?? undefined;
      existing.population = data.population;
      existing.lat = data.lat;
      existing.lng = data.lng;

      if (data.polygonWkt) {
        await this.updatePolygon(existing.id, data.polygonWkt);
      } else if (data.lat && data.lng) {
        await this.generateBufferPolygon(existing.id, data.lng, data.lat, data.type);
      }

      return this.geoRepository.save(existing);
    }

    const nextId = await this.geoRepository.getNextId();
    const maxRgt = await this.geoRepository.getMaxRgt();

    const geo: Partial<Geo> = {
      id: nextId,
      osmId: data.osmId,
      name: geoName,
      type: data.type,
      alias,
      parentId: parentId ?? undefined,
      population: data.population,
      lat: data.lat,
      lng: data.lng,
      lft: maxRgt + 1,
      rgt: maxRgt + 2,
      lvl: 1,
    };

    const saved = await this.geoRepository.save(geo);

    if (data.polygonWkt) {
      await this.updatePolygon(saved.id, data.polygonWkt);
    } else if (data.lat && data.lng) {
      await this.generateBufferPolygon(saved.id, data.lng, data.lat, data.type);
    }

    return saved;
  }

  private async updatePolygon(id: number, polygonWkt: string): Promise<void> {
    try {
      await this.geoRepository.updatePolygonFromWkt(id, polygonWkt);
    } catch (error) {
      this.logger.warn(`Failed to update polygon for ID ${id}: ${(error as Error).message}`);
    }
  }

  private async generateBufferPolygon(id: number, lng: number, lat: number, type: GeoType): Promise<void> {
    const radiusMeters = OSM_BUFFER_RADIUS[type] || 500;

    try {
      await this.geoRepository.generateBufferPolygon(id, lng, lat, radiusMeters);
    } catch (error) {
      this.logger.warn(`Failed to generate buffer polygon for ID ${id}: ${(error as Error).message}`);
    }
  }

  private async findParentId(data: ParsedGeoData, regionId: number): Promise<number> {
    if (data.type === GeoType.CityDistrict && data.polygonWkt) {
      const cityParentId = await this.geoRepository.findCityParentByPolygon(data.polygonWkt);

      if (cityParentId) {
        return cityParentId;
      }
    }

    if (data.lat && data.lng) {
      const parentId = await this.geoRepository.findParentByPoint(data.lng, data.lat);

      return parentId || regionId;
    }

    if (data.polygonWkt) {
      const parentId = await this.geoRepository.findParentByPolygonCentroid(data.polygonWkt);

      return parentId || regionId;
    }

    return regionId;
  }

  private groupByType(data: ParsedGeoData[]): Map<GeoType, ParsedGeoData[]> {
    const result = new Map<GeoType, ParsedGeoData[]>();

    for (const item of data) {
      if (!result.has(item.type)) {
        result.set(item.type, []);
      }

      result.get(item.type)!.push(item);
    }

    return result;
  }

  private async queryOverpass(query: string): Promise<OverpassResponse> {
    const url = this.configService.get<string>('OVERPASS_URL') || DEFAULT_OVERPASS_URL;

    const response = await lastValueFrom(
      this.httpService.post(url, `data=${encodeURIComponent(query)}`, {
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        timeout: 620000,
      }),
    );

    return response.data;
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
