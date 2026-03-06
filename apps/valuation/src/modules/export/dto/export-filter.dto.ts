import { ApiPropertyOptional } from '@nestjs/swagger';
import { IsOptional, IsInt, IsString, Min, Max } from 'class-validator';
import { Type } from 'class-transformer';

export class ExportRunDto {
  @ApiPropertyOptional({ default: 100 })
  @IsOptional()
  @IsInt()
  @Min(1)
  @Max(1000)
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

export class ExportStatsDto {
  total: number;
  exported: number;
  duplicate: number;
  error: number;
  skipped: number;
  pending: number;
}
