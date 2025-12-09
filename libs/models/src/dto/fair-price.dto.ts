import { ApiProperty } from '@nestjs/swagger';

export class PriceRangeDto {
  @ApiProperty({ description: 'Lower bound (Q1)' })
  low: number;

  @ApiProperty({ description: 'Upper bound (Q3)' })
  high: number;
}

export class PricePerMeterDto {
  @ApiProperty({ description: 'Median price per square meter' })
  median: number;

  @ApiProperty({ description: 'Average price per square meter' })
  average: number;
}

export class FairPriceDto {
  @ApiProperty({ description: 'Median price of analogs' })
  median: number;

  @ApiProperty({ description: 'Average price of analogs' })
  average: number;

  @ApiProperty({ description: 'Minimum price of analogs' })
  min: number;

  @ApiProperty({ description: 'Maximum price of analogs' })
  max: number;

  @ApiProperty({ type: PriceRangeDto, description: 'Price range (Q1-Q3)' })
  range: PriceRangeDto;

  @ApiProperty({ type: PricePerMeterDto, description: 'Price per meter statistics' })
  pricePerMeter: PricePerMeterDto;

  @ApiProperty({
    enum: ['cheap', 'in_market', 'expensive'],
    description: 'Price verdict relative to market',
  })
  verdict: 'cheap' | 'in_market' | 'expensive';

  @ApiProperty({ description: 'Number of analogs used for calculation' })
  analogsCount: number;
}
