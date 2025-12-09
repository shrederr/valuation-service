import { MultiLanguageDto } from '@libs/common';

// DTO for Geo events from vector-api
export class GeoEventDto {
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

export class GeoDeletedEventDto {
  public id: number;
}

// DTO for Street events from vector-api
export class StreetEventDto {
  public id: number;
  public name: MultiLanguageDto;
  public alias: string;
  public geoId: number;
  public bounds?: Record<string, number>;
  public coordinates?: number[][];
}

export class StreetDeletedEventDto {
  public id: number;
}

// DTO for Topzone events from vector-api
export class TopzoneEventDto {
  public id: number;
  public name: MultiLanguageDto;
  public alias: string;
  public lat?: number;
  public lng?: number;
  public bounds?: Record<string, number>;
  public declension?: MultiLanguageDto;
  public coordinates?: number[][][];
}

export class TopzoneDeletedEventDto {
  public id: number;
}

// DTO for ApartmentComplex events from vector-api
export class ComplexEventDto {
  public id: number;
  public name: string | MultiLanguageDto;
  public geoId?: number;
  public lat?: number;
  public lng?: number;
}

export class ComplexDeletedEventDto {
  public id: number;
}
