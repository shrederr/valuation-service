import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

/**
 * Attributes object matching vec.atlanta.ua /api/import-object endpoint.
 */
export interface Vector2ExportAttributes {
  secr_addres_hous_numb?: string;
  price: number;
  currency: string;
  currency_rent?: string;
  square_total?: number;
  square_living?: number;
  square_kitchen?: number;
  square_land_total?: number;
  rooms_count?: number;
  floor?: number;
  floors_count?: number;
  ceiling_height?: number;
  object_type?: number;
  apartment_type?: number;
  condition_type?: number;
  project?: number;
  housing_material?: number;
  map_x?: string;
  map_y?: string;
  description_rekl_ua?: string;
  description_rekl?: string;
  secr_owner_phone?: string;
  seller_name?: string;
  country1?: string;
}

/**
 * DTO matching vec.atlanta.ua /api/import-object endpoint.
 * Top-level fields + nested `attributes` object.
 */
export class Vector2ExportDto {
  @ApiProperty({ description: 'Our source_id (aggregator property ID)' })
  external_id: number;

  @ApiProperty({ description: 'type_estate: 1=apartment, 2=house, 3=commercial, 4=area, 5=garage' })
  type_estate: number;

  @ApiPropertyOptional({ description: 'CRM geo_id (resolved via source_id_mappings)' })
  geo_id?: number;

  @ApiPropertyOptional({ description: 'CRM street_id (resolved via source_id_mappings)' })
  street_id?: number;

  @ApiProperty({ description: 'Source platform name' })
  source_platform: string;

  @ApiPropertyOptional({ description: 'Original listing URL' })
  url?: string;

  @ApiPropertyOptional({ description: 'Photo URLs array' })
  photos?: string[];

  @ApiProperty({ description: 'All property attributes' })
  attributes: Vector2ExportAttributes;
}
