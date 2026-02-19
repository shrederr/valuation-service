import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { UnifiedListing } from '@libs/database';
import { SourceType } from '@libs/common';
import {
  VectorPropertyEventDto,
  VectorPropertyArchivedEventDto,
  VectorPropertyUnarchivedEventDto,
  AggregatorPropertyEventDto,
  AggregatorPropertyDeletedEventDto,
  Vector2ObjectRow,
} from '../dto';
import { VectorPropertyMapper } from '../mappers/vector-property.mapper';
import { AggregatorPropertyMapper } from '../mappers/aggregator-property.mapper';
import { Vector2PropertyMapper } from '../mappers/vector2-property.mapper';
import { InfrastructureService } from '../../infrastructure/infrastructure.service';
import { SourceIdMappingService } from './source-id-mapping.service';

@Injectable()
export class PropertySyncService {
  private readonly logger = new Logger(PropertySyncService.name);

  constructor(
    @InjectRepository(UnifiedListing)
    private readonly listingRepository: Repository<UnifiedListing>,
    private readonly vectorMapper: VectorPropertyMapper,
    private readonly aggregatorMapper: AggregatorPropertyMapper,
    private readonly vector2Mapper: Vector2PropertyMapper,
    private readonly infrastructureService: InfrastructureService,
    private readonly sourceIdMappingService: SourceIdMappingService,
  ) {}

  // === Vector Property Operations ===

  async handleVectorPropertyCreated(data: VectorPropertyEventDto): Promise<void> {
    try {
      const mapped = this.vectorMapper.mapToUnifiedListing(data);
      const listing = this.listingRepository.create(mapped);

      await this.listingRepository.save(listing);
      this.logger.log(`Vector property created: ${data.id} (unified: ${listing.id})`);

      if (listing.lat && listing.lng) {
        this.infrastructureService.updateListingInfrastructure(listing).catch((err) => {
          this.logger.warn(`Failed to fetch infrastructure for vector ${listing.id}: ${err.message}`);
        });
      }
    } catch (error) {
      this.logger.error(`Failed to create vector property ${data.id}`, error instanceof Error ? error.stack : undefined);
      throw error;
    }
  }

  async handleVectorPropertyUpdated(data: VectorPropertyEventDto): Promise<void> {
    try {
      const existing = await this.listingRepository.findOne({
        where: {
          sourceType: SourceType.VECTOR,
          sourceId: data.id,
        },
      });

      if (existing) {
        const mapped = this.vectorMapper.mapToUnifiedListing(data);
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        const { geo, street, topzone, complex, topzoneId, complexId, ...updateData } = mapped;
        const merged = this.listingRepository.merge(existing, updateData);
        await this.listingRepository.save(merged);
        this.logger.log(`Vector property updated: ${data.id}`);

        if (!existing.infrastructure && merged.lat && merged.lng) {
          this.infrastructureService.updateListingInfrastructure(merged).catch((err) => {
            this.logger.warn(`Failed to fetch infrastructure for vector ${merged.id}: ${err.message}`);
          });
        }
      } else {
        await this.handleVectorPropertyCreated(data);
      }
    } catch (error) {
      this.logger.error(`Failed to update vector property ${data.id}`, error instanceof Error ? error.stack : undefined);
      throw error;
    }
  }

  async handleVectorPropertyArchived(data: VectorPropertyArchivedEventDto): Promise<void> {
    try {
      await this.listingRepository.update(
        {
          sourceType: SourceType.VECTOR,
          sourceId: data.id,
        },
        {
          isActive: false,
          deletedAt: data.archivedAt ? new Date(data.archivedAt) : new Date(),
          syncedAt: new Date(),
        },
      );
      this.logger.log(`Vector property archived: ${data.id}`);
    } catch (error) {
      this.logger.error(`Failed to archive vector property ${data.id}`, error instanceof Error ? error.stack : undefined);
      throw error;
    }
  }

  async handleVectorPropertyUnarchived(data: VectorPropertyUnarchivedEventDto): Promise<void> {
    try {
      await this.listingRepository
        .createQueryBuilder()
        .update(UnifiedListing)
        .set({
          isActive: true,
          deletedAt: undefined,
          syncedAt: new Date(),
        })
        .where('sourceType = :sourceType AND sourceId = :sourceId', {
          sourceType: SourceType.VECTOR,
          sourceId: data.id,
        })
        .execute();
      this.logger.log(`Vector property unarchived: ${data.id}`);
    } catch (error) {
      this.logger.error(`Failed to unarchive vector property ${data.id}`, error instanceof Error ? error.stack : undefined);
      throw error;
    }
  }

  // === Aggregator Property Operations ===

  async handleAggregatorPropertyCreated(data: AggregatorPropertyEventDto): Promise<void> {
    try {
      this.logger.debug(
        `Aggregator property ${data.id} incoming data: lat=${data.lat}, lng=${data.lng}, geoId=${data.geoId}, streetId=${data.streetId}`,
      );

      // Check if already exists - treat as update
      const existing = await this.listingRepository.findOne({
        where: {
          sourceType: SourceType.AGGREGATOR,
          sourceId: data.id,
        },
      });

      if (existing) {
        return this.handleAggregatorPropertyUpdated(data);
      }

      const result = await this.aggregatorMapper.mapToUnifiedListing(data);
      // Strip TypeORM relation objects but keep resolved IDs (complexId is now resolved by ComplexMatcherService)
      const { geo, street, topzone, complex, topzoneId, ...listingData } = result.listing;
      const listing = this.listingRepository.create(listingData);

      await this.listingRepository.save(listing);

      const complexInfo = result.complexMatch
        ? `, complex: ${result.complexMatch.complexName} (${result.complexMatch.method})`
        : '';
      this.logger.log(
        `Aggregator property created: ${data.id} (unified: ${listing.id}, ` +
          `geoId: ${listing.geoId}, streetId: ${listing.streetId}, ` +
          `complexId: ${listing.complexId}${complexInfo}, ` +
          `condition: ${listing.condition}, houseType: ${listing.houseType})`,
      );

      // Fetch infrastructure asynchronously (fire-and-forget)
      if (listing.lat && listing.lng) {
        this.infrastructureService.updateListingInfrastructure(listing).catch((err) => {
          this.logger.warn(`Failed to fetch infrastructure for ${listing.id}: ${err.message}`);
        });
      }
    } catch (error) {
      this.logger.error(`Failed to create aggregator property ${data.id}`, error instanceof Error ? error.stack : undefined);
      throw error;
    }
  }

  async handleAggregatorPropertyUpdated(data: AggregatorPropertyEventDto): Promise<void> {
    try {
      const existing = await this.listingRepository.findOne({
        where: {
          sourceType: SourceType.AGGREGATOR,
          sourceId: data.id,
        },
      });

      if (existing) {
        const result = await this.aggregatorMapper.mapToUnifiedListing(data);
        // Strip TypeORM relation objects but keep resolved IDs
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        const { geo, street, topzone, complex, topzoneId, ...updateData } = result.listing;
        const merged = this.listingRepository.merge(existing, updateData);
        await this.listingRepository.save(merged);
        this.logger.log(
          `Aggregator property updated: ${data.id} ` +
            `(geoId: ${merged.geoId}, streetId: ${merged.streetId}, complexId: ${merged.complexId})`,
        );

        if (!existing.infrastructure && merged.lat && merged.lng) {
          this.infrastructureService.updateListingInfrastructure(merged).catch((err) => {
            this.logger.warn(`Failed to fetch infrastructure for aggregator ${merged.id}: ${err.message}`);
          });
        }
      } else if (data.isActive) {
        // Only create new record if property is active
        await this.handleAggregatorPropertyCreated(data);
      } else {
        this.logger.debug(`Skipping inactive aggregator property ${data.id} (not in DB)`);
      }
    } catch (error) {
      this.logger.error(`Failed to update aggregator property ${data.id}`, error instanceof Error ? error.stack : undefined);
      throw error;
    }
  }

  async handleAggregatorPropertyDeleted(data: AggregatorPropertyDeletedEventDto): Promise<void> {
    try {
      await this.listingRepository.update(
        {
          sourceType: SourceType.AGGREGATOR,
          sourceId: data.id,
        },
        {
          isActive: false,
          deletedAt: data.deletedAt ? new Date(data.deletedAt) : new Date(),
          syncedAt: new Date(),
        },
      );
      this.logger.log(`Aggregator property deleted: ${data.id}`);
    } catch (error) {
      this.logger.error(`Failed to delete aggregator property ${data.id}`, error instanceof Error ? error.stack : undefined);
      throw error;
    }
  }

  // === Vector2 CRM Property Operations ===

  async handleVector2PropertyUpsert(row: Vector2ObjectRow): Promise<string> {
    try {
      const idMappings = this.sourceIdMappingService.getMappings();
      const mapped = this.vector2Mapper.mapToUnifiedListing(row, idMappings);
      const existing = await this.listingRepository.findOne({
        where: {
          sourceType: SourceType.VECTOR_CRM,
          sourceId: row.id,
        },
      });

      if (existing) {
        const { geo, street, topzone, complex, ...updateData } = mapped;
        const merged = this.listingRepository.merge(existing, updateData);
        await this.listingRepository.save(merged);
        return existing.id;
      } else {
        const listing = this.listingRepository.create(mapped);
        await this.listingRepository.save(listing);
        return listing.id;
      }
    } catch (error) {
      this.logger.error(
        `Failed to upsert vector2 property ${row.id}`,
        error instanceof Error ? error.stack : undefined,
      );
      throw error;
    }
  }

  async handleVector2PropertyArchived(sourceId: number): Promise<void> {
    try {
      await this.listingRepository.update(
        {
          sourceType: SourceType.VECTOR_CRM,
          sourceId,
        },
        {
          isActive: false,
          deletedAt: new Date(),
          syncedAt: new Date(),
        },
      );
      this.logger.log(`Vector2 property archived: ${sourceId}`);
    } catch (error) {
      this.logger.error(
        `Failed to archive vector2 property ${sourceId}`,
        error instanceof Error ? error.stack : undefined,
      );
      throw error;
    }
  }

  /**
   * Batch upsert for vector2 objects — used during initial sync
   * Processes in chunks to avoid memory issues
   */
  async handleVector2PropertyBatch(rows: Vector2ObjectRow[]): Promise<{ created: number; updated: number; errors: number }> {
    let created = 0;
    let updated = 0;
    let errors = 0;
    const idMappings = this.sourceIdMappingService.getMappings();

    for (const row of rows) {
      try {
        const mapped = this.vector2Mapper.mapToUnifiedListing(row, idMappings);
        const existing = await this.listingRepository.findOne({
          where: {
            sourceType: SourceType.VECTOR_CRM,
            sourceId: row.id,
          },
        });

        if (existing) {
          const { geo, street, topzone, complex, ...updateData } = mapped;
          const merged = this.listingRepository.merge(existing, updateData);
          await this.listingRepository.save(merged);
          updated++;
        } else {
          const listing = this.listingRepository.create(mapped);
          await this.listingRepository.save(listing);
          created++;
        }
      } catch (error) {
        errors++;
        this.logger.warn(
          `Failed to upsert vector2 property ${row.id}: ${error instanceof Error ? error.message : error}`,
        );
      }
    }

    this.logger.log(`Vector2 batch complete: created=${created}, updated=${updated}, errors=${errors}`);
    return { created, updated, errors };
  }
}
