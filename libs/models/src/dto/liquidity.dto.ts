import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

export class LiquidityCriterionDto {
  @ApiProperty({ description: 'Criterion name' })
  public name: string;

  @ApiProperty({ description: 'Criterion weight (0-1)' })
  public weight: number;

  @ApiProperty({ description: 'Score for this criterion (0-10)' })
  public score: number;

  @ApiProperty({ description: 'Weighted score (weight * score)' })
  public weightedScore: number;

  @ApiPropertyOptional({ description: 'Explanation of score' })
  public explanation?: string;
}

export class LiquidityDto {
  @ApiProperty({ description: 'Overall liquidity score (0-10)' })
  public score: number;

  @ApiProperty({
    enum: ['high', 'medium', 'low'],
    description: 'Liquidity level',
  })
  public level: 'high' | 'medium' | 'low';

  @ApiProperty({
    type: [LiquidityCriterionDto],
    description: 'Breakdown by criteria',
  })
  public criteria: LiquidityCriterionDto[];

  @ApiProperty({ description: 'Estimated days to sell' })
  public estimatedDaysToSell: number;

  @ApiPropertyOptional({ description: 'Additional recommendations' })
  public recommendations?: string[];
}
