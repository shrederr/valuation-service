import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

export class Vector2ExportDto {
  @ApiProperty() source_id: number;
  @ApiProperty() source_platform: string;
  @ApiProperty() type_estate: number;
  @ApiPropertyOptional() fk_subcatid?: number;
  @ApiProperty() deal_type: string;
  @ApiPropertyOptional() fk_geo_id?: number;
  @ApiPropertyOptional() geo_street?: number;
  @ApiPropertyOptional() house_number?: string;
  @ApiPropertyOptional() map_x?: number;
  @ApiPropertyOptional() map_y?: number;
  @ApiProperty() price: number;
  @ApiPropertyOptional() price_per_meter?: number;
  @ApiProperty() currency: string;
  @ApiPropertyOptional() square_total?: number;
  @ApiPropertyOptional() square_living?: number;
  @ApiPropertyOptional() square_kitchen?: number;
  @ApiPropertyOptional() square_land_total?: number;
  @ApiPropertyOptional() rooms?: number;
  @ApiPropertyOptional() floor?: number;
  @ApiPropertyOptional() total_floors?: number;
  @ApiPropertyOptional() condition?: string;
  @ApiPropertyOptional() house_type?: string;
  @ApiPropertyOptional() description?: string;
  @ApiPropertyOptional() phones?: string[];
  @ApiPropertyOptional() photos?: string[];
  @ApiPropertyOptional() url?: string;
  @ApiProperty() is_active: boolean;
  @ApiPropertyOptional() published_at?: string;
  @ApiProperty() updated_at: string;
}
