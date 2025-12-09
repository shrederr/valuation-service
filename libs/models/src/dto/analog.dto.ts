import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { SourceType } from '@libs/common';

export class AnalogDto {
  @ApiProperty({ description: 'Unique identifier of the analog' })
  public id: string;

  @ApiProperty({ enum: SourceType, description: 'Source of the listing' })
  public source: SourceType;

  @ApiPropertyOptional({ description: 'Full address' })
  public address?: string;

  @ApiProperty({ description: 'Price in base currency' })
  public price: number;

  @ApiProperty({ description: 'Price per square meter' })
  public pricePerMeter: number;

  @ApiProperty({ description: 'Total area in square meters' })
  public area: number;

  @ApiPropertyOptional({ description: 'Number of rooms' })
  public rooms?: number;

  @ApiPropertyOptional({ description: 'Floor number' })
  public floor?: number;

  @ApiPropertyOptional({ description: 'Total floors in building' })
  public totalFloors?: number;

  @ApiPropertyOptional({ description: 'Property condition' })
  public condition?: string;

  @ApiPropertyOptional({ description: 'House type' })
  public houseType?: string;

  @ApiProperty({ description: 'Match score (0-1)' })
  public matchScore: number;

  @ApiPropertyOptional({ description: 'External URL' })
  public externalUrl?: string;
}

export class AnalogSearchResultDto {
  @ApiProperty({ type: [AnalogDto], description: 'List of found analogs' })
  public analogs: AnalogDto[];

  @ApiProperty({ description: 'Total count of analogs found' })
  public totalCount: number;

  @ApiProperty({
    enum: ['building', 'street', 'topzone', 'district', 'neighbor_districts', 'city'],
    description: 'Search radius used to find analogs',
  })
  public searchRadius: string;

  @ApiPropertyOptional({ description: 'Warning message if insufficient analogs' })
  public warning?: string;
}
