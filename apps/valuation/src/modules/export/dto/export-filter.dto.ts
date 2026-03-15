import { ApiPropertyOptional } from '@nestjs/swagger';
import { IsOptional, IsInt, IsString, Min, Max } from 'class-validator';
import { Type } from 'class-transformer';

export class ExportRunDto {
  @ApiPropertyOptional({ default: 500 })
  @IsOptional()
  @IsInt()
  @Min(1)
  @Max(5000)
  @Type(() => Number)
  batchSize?: number;

  @ApiPropertyOptional()
  @IsOptional()
  @IsString()
  realtyType?: string;

  @ApiPropertyOptional()
  @IsOptional()
  @IsInt()
  @Type(() => Number)
  geoId?: number;
}

export class ExportByPlatformsDto {
  @ApiPropertyOptional({ default: 25, description: 'Objects per platform' })
  @IsOptional()
  @IsInt()
  @Min(1)
  @Max(5000)
  @Type(() => Number)
  perPlatform?: number;

  @ApiPropertyOptional({
    description: 'Platforms to export from (default: olx, realtorUa, domRia, mlsUkraine)',
    type: [String],
  })
  @IsOptional()
  @IsString({ each: true })
  platforms?: string[];

  @ApiPropertyOptional()
  @IsOptional()
  @IsString()
  realtyType?: string;
}

export class ExportStatsDto {
  total: number;
  exported: number;
  duplicate: number;
  error: number;
  skipped: number;
  pending: number;
}
