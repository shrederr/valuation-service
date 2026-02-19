/**
 * DTO representing a row from vector2.object table (old CRM at vec.atlanta.ua)
 * Used for batch import — not from RabbitMQ events
 */
export interface Vector2ObjectRow {
  id: number;
  global_id?: string;
  type_estate: number; // 1=apartment, 2=house/land, 3=commercial
  fk_subcatid: number; // 6=house, 7=dacha, 20=land, etc.
  fk_geo_id: number;
  fk_geotop_id?: number;
  geo_street?: number;
  price?: string | number; // numeric → string from pg
  rent_price?: string | number;
  price_sqr?: string | number;
  currency_json?: Record<string, unknown>;
  square_total?: string | number;
  square_living?: string | number;
  square_land_total?: string | number;
  map_x?: string | number; // latitude
  map_y?: string | number; // longitude
  is_archive: boolean;
  time_create?: string | Date;
  time_update?: string | Date;
  attributes_data?: Record<string, unknown>;
  nearest_infrastructure?: Array<{
    lat: number;
    lng: number;
    type: string;
    distance: number;
    name?: string;
  }>;
  images?: unknown;
}
