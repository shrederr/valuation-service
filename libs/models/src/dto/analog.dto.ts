import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { SourceType } from '@libs/common';

export class AnalogDto {
  @ApiProperty({ description: 'Unique identifier of the analog' })
  id: string;

  @ApiProperty({ enum: SourceType, description: 'Source of the listing' })
  source: SourceType;

  @ApiPropertyOptional({ description: 'Full address' })
  address?: string;

  @ApiProperty({ description: 'Price in base currency' })
  price: number;

  @ApiProperty({ description: 'Price per square meter' })
  pricePerMeter: number;

  @ApiProperty({ description: 'Total area in square meters' })
  area: number;

  @ApiPropertyOptional({ description: 'Number of rooms' })
  rooms?: number;

  @ApiPropertyOptional({ description: 'Floor number' })
  floor?: number;

  @ApiPropertyOptional({ description: 'Total floors in building' })
  totalFloors?: number;

  @ApiPropertyOptional({ description: 'Property condition' })
  condition?: string;

  @ApiPropertyOptional({ description: 'House type' })
  houseType?: string;

  @ApiProperty({ description: 'Match score (0-1)' })
  matchScore: number;

  @ApiPropertyOptional({ description: 'External URL' })
  externalUrl?: string;
}

export class AnalogSearchResultDto {
  @ApiProperty({ type: [AnalogDto], description: 'List of found analogs' })
  analogs: AnalogDto[];

  @ApiProperty({ description: 'Total count of analogs found' })
  totalCount: number;

  @ApiProperty({
    enum: ['building', 'street', 'topzone', 'district', 'neighbor_districts', 'city'],
    description: 'Search radius used to find analogs',
  })
  searchRadius: string;

  @ApiPropertyOptional({ description: 'Warning message if insufficient analogs' })
  warning?: string;
}
