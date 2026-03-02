import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, ILike } from 'typeorm';
import { UnifiedListing } from '@libs/database';
import { SourceType } from '@libs/common';

@Injectable()
export class ListingsService {
  private readonly logger = new Logger(ListingsService.name);

  constructor(
    @InjectRepository(UnifiedListing)
    private readonly listingRepository: Repository<UnifiedListing>,
  ) {}

  async findByExternalUrl(url: string): Promise<UnifiedListing | null> {
    // Normalize URL - remove protocol and www
    const normalizedUrl = url
      .replace(/^https?:\/\//, '')
      .replace(/^www\./, '')
      .replace(/\/$/, '');

    this.logger.debug(`Searching for listing by external URL: ${normalizedUrl}`);

    // Search by exact match first
    let listing = await this.listingRepository.findOne({
      where: { externalUrl: url },
    });

    if (listing) {
      return listing;
    }

    // Try partial match (full normalized URL)
    listing = await this.listingRepository.findOne({
      where: { externalUrl: ILike(`%${normalizedUrl}%`) },
    });

    if (listing) {
      return listing;
    }

    // Try to extract numeric ID from URL and search by it
    // dom.ria.com: ...-33996497.html → 33996497
    // olx.ua: ...IDabcde.html → extract ID
    const numericIdMatch = normalizedUrl.match(/[-/](\d{5,})(?:\.html?)?$/);
    if (numericIdMatch) {
      const numericId = numericIdMatch[1];
      this.logger.debug(`Trying numeric ID from URL: ${numericId}`);

      listing = await this.listingRepository.findOne({
        where: { externalUrl: ILike(`%${numericId}%`) },
      });

      if (listing) {
        return listing;
      }

      // Also try as sourceId for aggregator
      const sourceId = parseInt(numericId, 10);
      if (!isNaN(sourceId)) {
        listing = await this.listingRepository.findOne({
          where: { sourceType: SourceType.AGGREGATOR, sourceId },
        });
      }
    }

    return listing;
  }

  async findById(id: string): Promise<UnifiedListing | null> {
    return this.listingRepository.findOne({
      where: { id },
      relations: ['geo', 'street'],
    });
  }

  async search(query: {
    external_url?: string;
    source_id?: number;
    source_type?: string;
  }): Promise<UnifiedListing | null> {
    if (query.external_url) {
      return this.findByExternalUrl(query.external_url);
    }

    if (query.source_id && query.source_type) {
      return this.listingRepository.findOne({
        where: {
          sourceId: query.source_id,
          sourceType: query.source_type as SourceType,
        },
      });
    }

    return null;
  }
}
