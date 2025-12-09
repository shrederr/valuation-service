export interface MultiLanguageFieldDto {
  uk: string;
  ru?: string;
  en?: string;
}

// DTO for Geo events from vector-api
export class GeoEventDto {
  id: number;
  name: MultiLanguageFieldDto;
  alias: string;
  type: string;
  lvl: number;
  lft: number;
  rgt: number;
  lat?: number;
  lng?: number;
  bounds?: Record<string, number>;
  declension?: MultiLanguageFieldDto;
}

export class GeoDeletedEventDto {
  id: number;
}

// DTO for Street events from vector-api
export class StreetEventDto {
  id: number;
  name: MultiLanguageFieldDto;
  alias: string;
  geoId: number;
  bounds?: Record<string, number>;
  coordinates?: number[][];
}

export class StreetDeletedEventDto {
  id: number;
}

// DTO for Topzone events from vector-api
export class TopzoneEventDto {
  id: number;
  name: MultiLanguageFieldDto;
  alias: string;
  lat?: number;
  lng?: number;
  bounds?: Record<string, number>;
  declension?: MultiLanguageFieldDto;
  coordinates?: number[][][];
}

export class TopzoneDeletedEventDto {
  id: number;
}

// DTO for ApartmentComplex events from vector-api
export class ComplexEventDto {
  id: number;
  name: string | MultiLanguageFieldDto;
  geoId?: number;
  lat?: number;
  lng?: number;
}

export class ComplexDeletedEventDto {
  id: number;
}
