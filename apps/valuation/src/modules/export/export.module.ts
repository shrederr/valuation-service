import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { HttpModule } from '@nestjs/axios';
import { ExportController } from './export.controller';
import { ExportService } from './export.service';
import { DedupCheckService } from './services/dedup-check.service';
import { EmbeddingService } from './services/embedding.service';
import { CrmClientService } from './services/crm-client.service';
import { PrimaryDataExtractor } from './services/primary-data-extractor';
import { TranslationService } from './services/translation.service';
import { ToCrmMapper } from './mappers/to-crm.mapper';

@Module({
  imports: [
    ConfigModule,
    HttpModule.register({ timeout: 25000 }),
  ],
  controllers: [ExportController],
  providers: [
    ExportService,
    DedupCheckService,
    EmbeddingService,
    CrmClientService,
    PrimaryDataExtractor,
    TranslationService,
    ToCrmMapper,
  ],
  exports: [ExportService, DedupCheckService],
})
export class ExportModule {}
