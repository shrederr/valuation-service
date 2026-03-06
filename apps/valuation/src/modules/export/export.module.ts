import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { ExportController } from './export.controller';
import { ExportService } from './export.service';
import { DedupCheckService } from './services/dedup-check.service';
import { EmbeddingService } from './services/embedding.service';
import { CrmClientService } from './services/crm-client.service';
import { PrimaryDataExtractor } from './services/primary-data-extractor';
import { ToCrmMapper } from './mappers/to-crm.mapper';

@Module({
  imports: [ConfigModule],
  controllers: [ExportController],
  providers: [
    ExportService,
    DedupCheckService,
    EmbeddingService,
    CrmClientService,
    PrimaryDataExtractor,
    ToCrmMapper,
  ],
  exports: [ExportService, DedupCheckService],
})
export class ExportModule {}
