import { ApiProperty } from '@nestjs/swagger';

export class PriceRangeDto {
  @ApiProperty({ description: 'Lower bound (Q1)' })
  public low: number;

  @ApiProperty({ description: 'Upper bound (Q3)' })
  public high: number;
}

export class PricePerMeterDto {
  @ApiProperty({ description: 'Median price per square meter' })
  public median: number;

  @ApiProperty({ description: 'Average price per square meter' })
  public average: number;
}

export class FairPriceDto {
  @ApiProperty({ description: 'Median price of analogs' })
  public median: number;

  @ApiProperty({ description: 'Average price of analogs' })
  public average: number;

  @ApiProperty({ description: 'Minimum price of analogs' })
  public min: number;

  @ApiProperty({ description: 'Maximum price of analogs' })
  public max: number;

  @ApiProperty({ type: PriceRangeDto, description: 'Price range (Q1-Q3)' })
  public range: PriceRangeDto;

  @ApiProperty({ type: PricePerMeterDto, description: 'Price per meter statistics' })
  public pricePerMeter: PricePerMeterDto;

  @ApiProperty({
    enum: ['cheap', 'in_market', 'expensive'],
    description: 'Price verdict relative to market',
  })
  public verdict: 'cheap' | 'in_market' | 'expensive';

  @ApiProperty({ description: 'Number of analogs used for calculation' })
  public analogsCount: number;
}
