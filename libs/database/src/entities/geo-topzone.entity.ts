import { Entity, PrimaryColumn, ManyToOne, JoinColumn } from 'typeorm';

import { Geo } from './geo.entity';
import { Topzone } from './topzone.entity';

@Entity('geo_topzones')
export class GeoTopzone {
  @PrimaryColumn({ name: 'geo_id', type: 'integer' })
  public geoId: number;

  @PrimaryColumn({ name: 'topzone_id', type: 'integer' })
  public topzoneId: number;

  @ManyToOne(() => Geo)
  @JoinColumn({ name: 'geo_id' })
  public geo?: Geo;

  @ManyToOne(() => Topzone)
  @JoinColumn({ name: 'topzone_id' })
  public topzone?: Topzone;
}

// Alias for backward compatibility
export { GeoTopzone as GeoTopzoneEntity };
