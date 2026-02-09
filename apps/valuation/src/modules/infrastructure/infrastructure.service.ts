import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, IsNull, Not } from 'typeorm';
import { firstValueFrom } from 'rxjs';

import { UnifiedListing } from '@libs/database/entities';

export type POIType = 'school' | 'hospital' | 'supermarket' | 'parking' | 'public_transport' | 'atm' | 'university' | 'post_office' | 'kindergarten';

export interface POIResult {
  lat: number;
  lng: number;
  type: POIType;
  distance: number;
  name?: string;
}

interface OverpassElement {
  id: number;
  type: 'node' | 'way' | 'relation';
  lat?: number;
  lon?: number;
  center?: { lat: number; lon: number };
  tags?: Record<string, string>;
}

@Injectable()
export class InfrastructureService {
  private readonly logger = new Logger(InfrastructureService.name);

  private readonly overpassEndpoints = [
    'https://overpass-api.de/api/interpreter',
    'https://z.overpass-api.de/api/interpreter',
    'https://overpass.kumi.systems/api/interpreter',
  ];

  constructor(
    private readonly httpService: HttpService,
    @InjectRepository(UnifiedListing)
    private readonly listingRepository: Repository<UnifiedListing>,
  ) {}

  private async postOverpass(query: string, timeoutMs = 30000): Promise<any> {
    let lastError: Error | null = null;

    for (const endpoint of this.overpassEndpoints) {
      try {
        const response = await firstValueFrom(
          this.httpService.post(endpoint, query, {
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            timeout: timeoutMs,
          }),
        );
        return response.data;
      } catch (e) {
        lastError = e as Error;
        this.logger.warn(`Overpass endpoint failed: ${endpoint} (${lastError?.message})`);
        continue;
      }
    }

    throw lastError ?? new Error('All Overpass endpoints failed');
  }

  private haversineDistance(lat1: number, lon1: number, lat2: number, lon2: number): number {
    const R = 6371000; // Earth radius in meters
    const toRad = (deg: number) => (deg * Math.PI) / 180;

    const dLat = toRad(lat2 - lat1);
    const dLon = toRad(lon2 - lon1);

    const a =
      Math.sin(dLat / 2) * Math.sin(dLat / 2) +
      Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) * Math.sin(dLon / 2);

    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    return Math.round(R * c);
  }

  async getInfrastructureByCoordinates(lat: number, lng: number, radius = 1000): Promise<POIResult[]> {
    const query = `
[out:json][timeout:25];
(
  // Schools and kindergartens
  nwr(around:${radius},${lat},${lng})["amenity"~"^(school|kindergarten)$"];

  // Hospitals and clinics
  nwr(around:${radius},${lat},${lng})["amenity"~"^(hospital|clinic)$"];
  nwr(around:${radius},${lat},${lng})["healthcare"~"^(hospital|clinic|doctor)$"];

  // Supermarkets
  nwr(around:${radius},${lat},${lng})["shop"="supermarket"];

  // Parking
  nwr(around:${radius},${lat},${lng})["amenity"="parking"];

  // Public transport (bus stops, tram stops, metro)
  nwr(around:${radius},${lat},${lng})["public_transport"~"^(stop_position|platform|station)$"];
  nwr(around:${radius},${lat},${lng})["highway"="bus_stop"];
  nwr(around:${radius},${lat},${lng})["railway"~"^(station|halt|tram_stop|subway_entrance)$"];
);
out center;`;

    try {
      const data = await this.postOverpass(query);
      const elements: OverpassElement[] = data?.elements ?? [];

      return this.mapElements(elements, lat, lng);
    } catch (e) {
      this.logger.error(`Failed to fetch infrastructure for ${lat},${lng}`, e);
      return [];
    }
  }

  private mapElements(elements: OverpassElement[], originLat: number, originLng: number): POIResult[] {
    const results: POIResult[] = [];

    for (const el of elements) {
      const tags = el.tags ?? {};
      let type: POIType | null = null;

      // Determine type
      if (tags.amenity === 'school') type = 'school';
      else if (tags.amenity === 'kindergarten') type = 'kindergarten';
      else if (tags.amenity === 'hospital' || tags.healthcare === 'hospital') type = 'hospital';
      else if (tags.amenity === 'clinic' || tags.healthcare === 'clinic' || tags.healthcare === 'doctor') type = 'hospital';
      else if (tags.shop === 'supermarket') type = 'supermarket';
      else if (tags.amenity === 'parking') type = 'parking';
      else if (
        tags.public_transport ||
        tags.highway === 'bus_stop' ||
        tags.railway === 'station' ||
        tags.railway === 'halt' ||
        tags.railway === 'tram_stop' ||
        tags.railway === 'subway_entrance'
      ) {
        type = 'public_transport';
      }

      if (!type) continue;

      // Get coordinates
      const elLat = el.lat ?? el.center?.lat;
      const elLng = el.lon ?? el.center?.lon;

      if (!elLat || !elLng) continue;

      const distance = this.haversineDistance(originLat, originLng, elLat, elLng);
      const name = tags.name || tags['name:uk'] || tags['name:ru'];

      results.push({ lat: elLat, lng: elLng, type, distance, name });
    }

    // Sort by distance
    return results.sort((a, b) => a.distance - b.distance);
  }

  async updateListingInfrastructure(listing: UnifiedListing): Promise<void> {
    if (!listing.lat || !listing.lng) return;

    const infrastructure = await this.getInfrastructureByCoordinates(
      Number(listing.lat),
      Number(listing.lng),
    );

    if (infrastructure.length === 0) return;

    // Find nearest of each type
    const nearestByType: Record<POIType, number | undefined> = {
      school: undefined,
      hospital: undefined,
      supermarket: undefined,
      parking: undefined,
      public_transport: undefined,
      atm: undefined,
      university: undefined,
      post_office: undefined,
      kindergarten: undefined,
    };

    for (const poi of infrastructure) {
      if (nearestByType[poi.type] === undefined) {
        nearestByType[poi.type] = poi.distance;
      }
    }

    await this.listingRepository.update(listing.id, {
      nearestSchool: nearestByType.school ?? nearestByType.kindergarten,
      nearestHospital: nearestByType.hospital,
      nearestSupermarket: nearestByType.supermarket,
      nearestParking: nearestByType.parking,
      nearestPublicTransport: nearestByType.public_transport,
      infrastructure: infrastructure.slice(0, 20), // Keep top 20 nearest
    });
  }

  async processListingsBatch(batchSize = 100, delayMs = 1000): Promise<{ processed: number; updated: number }> {
    let processed = 0;
    let updated = 0;
    let offset = 0;

    while (true) {
      // Get listings without infrastructure data
      const listings = await this.listingRepository.find({
        where: {
          lat: Not(IsNull()),
          lng: Not(IsNull()),
          infrastructure: IsNull(),
        },
        select: ['id', 'lat', 'lng'],
        take: batchSize,
        skip: offset,
      });

      if (listings.length === 0) break;

      for (const listing of listings) {
        try {
          await this.updateListingInfrastructure(listing);
          updated++;
        } catch (e) {
          this.logger.error(`Failed to update infrastructure for ${listing.id}`, e);
        }
        processed++;

        // Rate limiting
        if (processed % 10 === 0) {
          await this.sleep(delayMs);
        }
      }

      this.logger.log(`Infrastructure batch progress: ${processed} processed, ${updated} updated`);
      offset += batchSize;
    }

    return { processed, updated };
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  async getListingsWithoutInfrastructureCount(): Promise<number> {
    return this.listingRepository.count({
      where: {
        lat: Not(IsNull()),
        lng: Not(IsNull()),
        infrastructure: IsNull(),
      },
    });
  }
}
