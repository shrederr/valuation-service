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
} from '../dto';
import { VectorPropertyMapper } from '../mappers/vector-property.mapper';
import { AggregatorPropertyMapper } from '../mappers/aggregator-property.mapper';

@Injectable()
export class PropertySyncService {
  private readonly logger = new Logger(PropertySyncService.name);

  constructor(
    @InjectRepository(UnifiedListing)
    private readonly listingRepository: Repository<UnifiedListing>,
    private readonly vectorMapper: VectorPropertyMapper,
    private readonly aggregatorMapper: AggregatorPropertyMapper,
  ) {}

  // === Vector Property Operations ===

  async handleVectorPropertyCreated(data: VectorPropertyEventDto): Promise<void> {
    try {
      const mapped = this.vectorMapper.mapToUnifiedListing(data);
      const listing = this.listingRepository.create(mapped);

      await this.listingRepository.save(listing);
      this.logger.log(`Vector property created: ${data.id} (unified: ${listing.id})`);
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
        const { geo, street, topzone, complex, ...updateData } = mapped;
        const merged = this.listingRepository.merge(existing, updateData);
        await this.listingRepository.save(merged);
        this.logger.log(`Vector property updated: ${data.id}`);
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
      const result = await this.aggregatorMapper.mapToUnifiedListing(data);
      const listing = this.listingRepository.create(result.listing);

      await this.listingRepository.save(listing);
      this.logger.log(
        `Aggregator property created: ${data.id} (unified: ${listing.id}, ` +
          `geoId: ${listing.geoId}, streetId: ${listing.streetId}, ` +
          `condition: ${listing.condition}, houseType: ${listing.houseType})`,
      );
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
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        const { geo, street, topzone, complex, ...updateData } = result.listing;
        const merged = this.listingRepository.merge(existing, updateData);
        await this.listingRepository.save(merged);
        this.logger.log(
          `Aggregator property updated: ${data.id} ` +
            `(geoId: ${merged.geoId}, streetId: ${merged.streetId})`,
        );
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
}
