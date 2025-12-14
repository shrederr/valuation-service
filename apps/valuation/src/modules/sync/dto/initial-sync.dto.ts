import { MultiLanguageDto } from '@libs/common';

export class PaginatedResponseDto<T> {
  public items: T[];
  public total: number;
  public page: number;
  public pageSize: number;
}

export class VectorGeoDto {
  public id: number;
  public name: MultiLanguageDto;
  public alias: string;
  public type: string;
  public lvl: number;
  public lft: number;
  public rgt: number;
  public lat?: number;
  public lng?: number;
  public bounds?: Record<string, number>;
  public declension?: MultiLanguageDto;
}

export class VectorStreetDto {
  public id: number;
  public name: MultiLanguageDto;
  public alias: string;
  public geoId: number;
  public bounds?: Record<string, number>;
  public coordinates?: number[][];
}

export class VectorTopzoneDto {
  public id: number;
  public name: MultiLanguageDto;
  public alias: string;
  public lat?: number;
  public lng?: number;
  public bounds?: Record<string, number>;
  public declension?: MultiLanguageDto;
  public coordinates?: number[][][];
}

export class VectorComplexDto {
  public id: number;
  public name: MultiLanguageDto | string;
  public geoId?: number;
  public topzoneId?: number;
  public lat?: number;
  public lng?: number;
  public type?: number;
}

export class VectorPropertyDto {
  public id: number;
  public globalId: string;
  public dealType: string;
  public realtyType: string;
  public realtySubtype?: string;
  public geoId: number;
  public streetId: number;
  public topzoneId: number;
  public complexId: number;
  public houseNumber: string;
  public apartmentNumber?: string;
  public corps?: string;
  public cadastralNumber?: string;
  public lat?: number;
  public lng?: number;
  public attributes?: Record<string, unknown>;
  public isArchived?: boolean;
  public createdAt: string;
  public updatedAt: string;
}

export class AggregatorPropertyDto {
  public id: number;
  public externalId: string;
  public dealType: string;
  public realtyType: string;
  public geoId: number;
  public streetId: number;
  public topzoneId: number;
  public complexId: number;
  public houseNumber: string;
  public lat?: number;
  public lng?: number;
  public price: number;
  public currency: string;
  public attributes?: Record<string, unknown>;
  public description?: Record<string, string>;
  public url?: string;
  public isActive: boolean;
  public createdAt: string;
  public updatedAt: string;
}
