// DTO for CustomerProperty events from vector-api
export class VectorPropertyEventDto {
  public id: number;
  public globalId: string;
  public dealType: string;
  public realtyType: string;
  public realtySubtype: string;
  public geoId: number;
  public streetId: number;
  public topzoneId: number;
  public complexId: number;
  public houseNumber: string;
  public houseNumberAdd: string;
  public apartmentNumber: string;
  public corps: string;
  public cadastralNumber: string;
  public lat: number;
  public lng: number;
  public attributes: Record<string, unknown>;
  public primaryData?: Record<string, unknown>;
  public createdAt: Date;
  public updatedAt: Date;
}

export class VectorPropertyArchivedEventDto {
  public id: number;
  public globalId: string;
  public archivedAt: Date;
}

export class VectorPropertyUnarchivedEventDto {
  public id: number;
  public globalId: string;
}

// DTO for ExportedProperty events from api-property-aggregator
export class AggregatorPropertyEventDto {
  public id: number;
  public externalId: string;
  public dealType: string;
  public realtyType: string;
  public realtyPlatform: string;
  public geoId: number;
  public streetId: number;
  public topzoneId: number;
  public complexId: number;
  public houseNumber: string;
  public lat: number;
  public lng: number;
  public price: number;
  public currency: string;
  public attributes: Record<string, unknown>;
  public primaryData?: Record<string, unknown>;
  public seller: Record<string, unknown>;
  public description: Record<string, string>;
  public images: string[];
  public url: string;
  public hash: string;
  public isActive: boolean;
  public isExported: boolean;
  public createdAt: Date;
  public updatedAt: Date;
  public deletedAt: Date;
}

export class AggregatorPropertyDeletedEventDto {
  public id: number;
  public deletedAt: Date;
}
