import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

export class LiquidityCriterionDto {
  @ApiProperty({ description: 'Criterion name' })
  name: string;

  @ApiProperty({ description: 'Criterion weight (0-1)' })
  weight: number;

  @ApiProperty({ description: 'Score for this criterion (0-10)' })
  score: number;

  @ApiProperty({ description: 'Weighted score (weight * score)' })
  weightedScore: number;

  @ApiPropertyOptional({ description: 'Explanation of score' })
  explanation?: string;
}

export class LiquidityDto {
  @ApiProperty({ description: 'Overall liquidity score (0-10)' })
  score: number;

  @ApiProperty({
    enum: ['high', 'medium', 'low'],
    description: 'Liquidity level',
  })
  level: 'high' | 'medium' | 'low';

  @ApiProperty({
    type: [LiquidityCriterionDto],
    description: 'Breakdown by criteria',
  })
  criteria: LiquidityCriterionDto[];

  @ApiProperty({ description: 'Estimated days to sell' })
  estimatedDaysToSell: number;

  @ApiPropertyOptional({ description: 'Additional recommendations' })
  recommendations?: string[];
}
