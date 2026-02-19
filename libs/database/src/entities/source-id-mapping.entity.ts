import { Entity, Column, PrimaryGeneratedColumn, Index, CreateDateColumn } from 'typeorm';

/**
 * Cross-reference mapping between external source IDs and our local IDs.
 * Used to map geo, streets, and apartment complexes from different systems
 * (e.g. old vector CRM at vec.atlanta.ua) to our database.
 */
@Entity('source_id_mappings')
@Index(['source', 'entityType', 'sourceId'], { unique: true })
@Index(['source', 'entityType', 'localId'])
export class SourceIdMapping {
  @PrimaryGeneratedColumn()
  public id: number;

  /** External system identifier, e.g. 'vector2_crm' */
  @Column({ type: 'varchar', length: 30 })
  public source: string;

  /** Entity type: 'geo', 'street', 'complex', 'topzone' */
  @Column({ name: 'entity_type', type: 'varchar', length: 20 })
  public entityType: string;

  /** ID in the external system */
  @Column({ name: 'source_id', type: 'integer' })
  public sourceId: number;

  /** Corresponding ID in our database */
  @Column({ name: 'local_id', type: 'integer' })
  public localId: number;

  /** Match confidence: 1.0 = exact, < 1.0 = fuzzy */
  @Column({ type: 'decimal', precision: 3, scale: 2, default: 1.0 })
  public confidence: number;

  /** How the match was made: 'exact_name', 'fuzzy_name', 'coordinates', 'manual' */
  @Column({ name: 'match_method', type: 'varchar', length: 30, nullable: true })
  public matchMethod?: string;

  @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
  public createdAt: Date;
}
