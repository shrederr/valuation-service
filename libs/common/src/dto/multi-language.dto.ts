import { ApiProperty } from '@nestjs/swagger';
import { IsOptional, IsString } from 'class-validator';

export class MultiLanguageDto {
  @ApiProperty({ required: true })
  @IsString()
  public uk: string;

  @ApiProperty({ required: false })
  @IsOptional()
  @IsString()
  public ru?: string;

  @ApiProperty({ required: false })
  @IsOptional()
  @IsString()
  public en?: string;
}
