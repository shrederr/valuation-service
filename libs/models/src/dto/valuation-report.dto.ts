import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { AnalogSearchResultDto } from './analog.dto';
import { FairPriceDto } from './fair-price.dto';
import { LiquidityDto } from './liquidity.dto';

export class PropertyInfoDto {
  @ApiProperty({ description: 'Property ID from source system' })
  sourceId: number;

  @ApiProperty({ description: 'Source system identifier' })
  sourceType: string;

  @ApiPropertyOptional({ description: 'Property address' })
  address?: string;

  @ApiProperty({ description: 'Property area in square meters' })
  area: number;

  @ApiPropertyOptional({ description: 'Number of rooms' })
  rooms?: number;

  @ApiPropertyOptional({ description: 'Floor number' })
  floor?: number;

  @ApiPropertyOptional({ description: 'Total floors in building' })
  totalFloors?: number;

  @ApiPropertyOptional({ description: 'Asking price if available' })
  askingPrice?: number;
}

export class ValuationReportDto {
  @ApiProperty({ description: 'Unique report ID' })
  reportId: string;

  @ApiProperty({ description: 'Report generation timestamp' })
  generatedAt: Date;

  @ApiProperty({ type: PropertyInfoDto, description: 'Evaluated property info' })
  property: PropertyInfoDto;

  @ApiProperty({ type: FairPriceDto, description: 'Fair price calculation results' })
  fairPrice: FairPriceDto;

  @ApiProperty({ type: LiquidityDto, description: 'Liquidity assessment' })
  liquidity: LiquidityDto;

  @ApiProperty({ type: AnalogSearchResultDto, description: 'Analog search results' })
  analogs: AnalogSearchResultDto;

  @ApiPropertyOptional({ description: 'Confidence level of valuation (0-1)' })
  confidence?: number;

  @ApiPropertyOptional({ description: 'Warnings or notes about the valuation' })
  notes?: string[];
}

export class ValuationRequestDto {
  @ApiProperty({ description: 'Source system (vector or aggregator)' })
  sourceType: string;

  @ApiProperty({ description: 'Property ID in source system' })
  sourceId: number;

  @ApiPropertyOptional({ description: 'Force recalculation even if cached' })
  forceRefresh?: boolean;
}

export class BatchValuationRequestDto {
  @ApiProperty({
    type: [ValuationRequestDto],
    description: 'List of properties to valuate',
  })
  properties: ValuationRequestDto[];
}

export class BatchValuationResponseDto {
  @ApiProperty({
    type: [ValuationReportDto],
    description: 'List of valuation reports',
  })
  reports: ValuationReportDto[];

  @ApiProperty({ description: 'Number of successful valuations' })
  successCount: number;

  @ApiProperty({ description: 'Number of failed valuations' })
  failedCount: number;

  @ApiPropertyOptional({
    description: 'List of failed property IDs with error messages',
  })
  failures?: Array<{ sourceType: string; sourceId: number; error: string }>;
}
