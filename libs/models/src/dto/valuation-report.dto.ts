import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { AnalogSearchResultDto } from './analog.dto';
import { FairPriceDto } from './fair-price.dto';
import { LiquidityDto } from './liquidity.dto';

export class PropertyInfoDto {
  @ApiProperty({ description: 'Property ID from source system' })
  public sourceId: number;

  @ApiProperty({ description: 'Source system identifier' })
  public sourceType: string;

  @ApiPropertyOptional({ description: 'Property address' })
  public address?: string;

  @ApiProperty({ description: 'Property area in square meters' })
  public area: number;

  @ApiPropertyOptional({ description: 'Number of rooms' })
  public rooms?: number;

  @ApiPropertyOptional({ description: 'Floor number' })
  public floor?: number;

  @ApiPropertyOptional({ description: 'Total floors in building' })
  public totalFloors?: number;

  @ApiPropertyOptional({ description: 'Asking price if available' })
  public askingPrice?: number;
}

export class ValuationReportDto {
  @ApiProperty({ description: 'Unique report ID' })
  public reportId: string;

  @ApiProperty({ description: 'Report generation timestamp' })
  public generatedAt: Date;

  @ApiProperty({ type: PropertyInfoDto, description: 'Evaluated property info' })
  public property: PropertyInfoDto;

  @ApiProperty({ type: FairPriceDto, description: 'Fair price calculation results' })
  public fairPrice: FairPriceDto;

  @ApiProperty({ type: LiquidityDto, description: 'Liquidity assessment' })
  public liquidity: LiquidityDto;

  @ApiProperty({ type: AnalogSearchResultDto, description: 'Analog search results' })
  public analogs: AnalogSearchResultDto;

  @ApiPropertyOptional({ description: 'Confidence level of valuation (0-1)' })
  public confidence?: number;

  @ApiPropertyOptional({ description: 'Warnings or notes about the valuation' })
  public notes?: string[];
}

export class ValuationRequestDto {
  @ApiProperty({ description: 'Source system (vector or aggregator)' })
  public sourceType: string;

  @ApiProperty({ description: 'Property ID in source system' })
  public sourceId: number;

  @ApiPropertyOptional({ description: 'Force recalculation even if cached' })
  public forceRefresh?: boolean;
}

export class BatchValuationRequestDto {
  @ApiProperty({
    type: [ValuationRequestDto],
    description: 'List of properties to valuate',
  })
  public properties: ValuationRequestDto[];
}

export class BatchValuationResponseDto {
  @ApiProperty({
    type: [ValuationReportDto],
    description: 'List of valuation reports',
  })
  public reports: ValuationReportDto[];

  @ApiProperty({ description: 'Number of successful valuations' })
  public successCount: number;

  @ApiProperty({ description: 'Number of failed valuations' })
  public failedCount: number;

  @ApiPropertyOptional({
    description: 'List of failed property IDs with error messages',
  })
  public failures?: Array<{ sourceType: string; sourceId: number; error: string }>;
}
