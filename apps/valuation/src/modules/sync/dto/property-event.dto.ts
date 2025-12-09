// DTO for CustomerProperty events from vector-api
export class VectorPropertyEventDto {
  id: number;
  globalId: string;
  dealType: string;
  realtyType: string;
  realtySubtype: string;
  geoId: number;
  streetId: number;
  topzoneId: number;
  complexId: number;
  houseNumber: string;
  houseNumberAdd: string;
  apartmentNumber: string;
  corps: string;
  cadastralNumber: string;
  lat: number;
  lng: number;
  attributes: Record<string, any>;
  createdAt: Date;
  updatedAt: Date;
}

export class VectorPropertyArchivedEventDto {
  id: number;
  globalId: string;
  archivedAt: Date;
}

export class VectorPropertyUnarchivedEventDto {
  id: number;
  globalId: string;
}

// DTO for ExportedProperty events from api-property-aggregator
export class AggregatorPropertyEventDto {
  id: number;
  externalId: string;
  dealType: string;
  realtyType: string;
  realtyPlatform: string;
  geoId: number;
  streetId: number;
  topzoneId: number;
  complexId: number;
  houseNumber: string;
  lat: number;
  lng: number;
  price: number;
  currency: string;
  attributes: Record<string, any>;
  seller: Record<string, any>;
  description: Record<string, string>;
  images: string[];
  url: string;
  hash: string;
  isActive: boolean;
  isExported: boolean;
  createdAt: Date;
  updatedAt: Date;
  deletedAt: Date;
}

export class AggregatorPropertyDeletedEventDto {
  id: number;
  deletedAt: Date;
}
