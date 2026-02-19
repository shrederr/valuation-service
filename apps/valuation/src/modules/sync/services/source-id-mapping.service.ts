import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { SourceIdMapping } from '@libs/database';
import { Vector2IdMappings } from '../mappers/vector2-property.mapper';

const SOURCE = 'vector2_crm';

@Injectable()
export class SourceIdMappingService implements OnModuleInit {
  private readonly logger = new Logger(SourceIdMappingService.name);
  private mappings: Vector2IdMappings = {
    geo: new Map(),
    street: new Map(),
    complex: new Map(),
  };

  constructor(
    @InjectRepository(SourceIdMapping)
    private readonly repo: Repository<SourceIdMapping>,
  ) {}

  async onModuleInit(): Promise<void> {
    await this.reload();
  }

  getMappings(): Vector2IdMappings {
    return this.mappings;
  }

  async reload(): Promise<void> {
    const rows = await this.repo.find({ where: { source: SOURCE } });

    const geo = new Map<number, number>();
    const street = new Map<number, number>();
    const complex = new Map<number, number>();

    for (const row of rows) {
      switch (row.entityType) {
        case 'geo':
          geo.set(row.sourceId, row.localId);
          break;
        case 'street':
          street.set(row.sourceId, row.localId);
          break;
        case 'complex':
          complex.set(row.sourceId, row.localId);
          break;
      }
    }

    this.mappings = { geo, street, complex };
    this.logger.log(
      `Loaded ${SOURCE} mappings: geo=${geo.size}, street=${street.size}, complex=${complex.size}`,
    );
  }
}
