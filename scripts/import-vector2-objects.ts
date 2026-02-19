/**
 * Import vector2 objects from JSONL dump into unified_listings.
 *
 * 1. Loads source_id_mappings (geo/street/complex) from local DB
 * 2. Reads vector2_objects.jsonl.gz line by line
 * 3. Maps each object via Vector2PropertyMapper (with ID resolution)
 * 4. Bulk-inserts into unified_listings (ON CONFLICT UPDATE)
 *
 * Usage: npx ts-node --transpile-only scripts/import-vector2-objects.ts
 *
 * Prerequisites:
 *   - $TEMP/vector2_objects.jsonl.gz (from vec.atlanta.ua vector2.object)
 *   - source_id_mappings populated (run match-vector2-ids.ts + match-via-osm-id.ts first)
 *   - Local DB running on localhost:5433
 */

import * as fs from 'fs';
import * as zlib from 'zlib';
import * as readline from 'readline';
import { Client } from 'pg';

// ============================================================
// Inline mapper logic (to avoid NestJS DI in a standalone script)
// ============================================================

const CONDITION_MAP: Record<number, string> = {
  1: 'Потрібен капітальний ремонт', 2: 'Потрібен поточний ремонт',
  3: 'Потрібен косметичний ремонт', 4: 'Після капремонту',
  5: 'Євроремонт', 6: 'Будинок, що будується', 7: 'Після будівельників',
  20: 'Житлове чисте', 21: 'Після косметики', 22: 'Ремонт не потрібний', 24: 'White Box',
};

const PROJECT_MAP: Record<number, string> = {
  1: 'Сталінка', 2: 'Старий фонд', 3: 'Висотний будинок', 4: 'Хрущовка',
  5: 'Гостинка', 7: 'Новобуд', 8: 'Чеська', 9: 'Моноліт', 10: 'Спецпроект',
  11: 'Московський', 14: 'Болгарська', 15: 'Малосімейка',
  33: 'Царський (дореволюційний)', 34: 'Польська', 37: 'Австрійська', 39: 'Київка',
};

const MATERIAL_MAP: Record<number, string> = {
  1: 'Бетон', 2: 'Цегла', 4: 'Ракушняк', 5: 'Силікатна цегла', 6: 'Дерево',
  7: 'Панельний', 8: 'Блоковий', 26: 'Моноліт', 27: 'Пінобетон', 28: 'Газобетон',
};

const PLANNING_MAP: Record<number, string> = {
  1: 'Роздільна', 2: 'Суміжна', 3: 'Суміжно-роздільна', 4: 'Розпашонка',
  7: 'Гостинка', 8: 'Малосімейка', 16: '2 рівні', 24: 'Вільне планування',
};

const HEATING_MAP: Record<number, string> = {
  1: 'ТЕЦ', 2: 'АГВ', 3: 'Пічне', 4: 'Електроопалення',
  5: 'Автономне опалення', 6: 'Без опалення', 7: 'Котел',
};

type DealType = 'sell' | 'rent';
type RealtyType = 'apartment' | 'house' | 'commercial' | 'area';

interface Vector2Row {
  id: number;
  type_estate: number;
  fk_subcatid: number;
  fk_geo_id: number;
  fk_geotop_id: number | null;
  geo_street: number | null;
  price: string | number | null;
  rent_price: string | number | null;
  price_sqr: string | number | null;
  currency_json: Record<string, unknown> | null;
  square_total: string | number | null;
  square_living: string | number | null;
  square_land_total: string | number | null;
  map_x: string | number | null;
  map_y: string | number | null;
  attributes_data: Record<string, unknown> | null;
  nearest_infrastructure: Array<{ type: string; distance: number; lat: number; lng: number }> | null;
  is_archive: boolean;
  is_exclusive: number | boolean;
  time_create: string | null;
  time_update: string | null;
  global_id: string | null;
}

// ============================================================
// Helpers
// ============================================================

function num(v: unknown): number | null {
  if (v === null || v === undefined) return null;
  const n = typeof v === 'number' ? v : parseFloat(String(v));
  return isNaN(n) ? null : n;
}

function int(v: unknown): number | null {
  if (v === null || v === undefined) return null;
  const n = typeof v === 'number' ? Math.floor(v) : parseInt(String(v), 10);
  return isNaN(n) ? null : n;
}

function mapDict(value: unknown, dict: Record<number, string>): string | null {
  const id = int(value);
  if (id === null) return null;
  return dict[id] || null;
}

function mapRealtyType(typeEstate: number, subcatid: number, attrs: Record<string, unknown>): RealtyType {
  if (typeEstate === 1) return 'apartment';
  if (typeEstate === 3) return 'commercial';
  if (typeEstate === 2) {
    if (subcatid === 20) return 'area';
    const objType = int(attrs.object_type);
    if (objType === 5) return 'area';
    return 'house';
  }
  return 'apartment';
}

function mapDealType(row: Vector2Row): DealType {
  const rent = num(row.rent_price);
  const sell = num(row.price);
  if (rent && rent > 0 && (!sell || sell === 0)) return 'rent';
  return 'sell';
}

function getPrice(row: Vector2Row): number | null {
  const rent = num(row.rent_price);
  const sell = num(row.price);
  if (rent && rent > 0 && (!sell || sell === 0)) return rent;
  return sell;
}

function extractCurrency(row: Vector2Row, attrs: Record<string, unknown>): string {
  if (row.currency_json && typeof row.currency_json === 'object') {
    const code = (row.currency_json as Record<string, unknown>).currency;
    if (typeof code === 'string' && ['USD', 'EUR', 'UAH'].includes(code.toUpperCase())) {
      return code.toUpperCase();
    }
  }
  const ac = attrs.currency;
  if (typeof ac === 'string') {
    const u = ac.toUpperCase();
    if (u === 'USD' || u === 'EUR' || u === 'UAH') return u;
  }
  return 'USD';
}

function extractNearestDistances(infra: any): Record<string, number> {
  if (!infra || !Array.isArray(infra)) return {};
  const result: Record<string, number> = {};
  const typeMap: Record<string, string> = {
    school: 'nearest_school', hospital: 'nearest_hospital',
    supermarket: 'nearest_supermarket', parking: 'nearest_parking',
    bus_station: 'nearest_public_transport', tram_stop: 'nearest_public_transport',
    trolleybus_stop: 'nearest_public_transport',
  };
  for (const item of infra) {
    const field = typeMap[item.type];
    if (field && typeof item.distance === 'number') {
      if (!result[field] || item.distance < result[field]) {
        result[field] = Math.round(item.distance);
      }
    }
  }
  return result;
}

// ============================================================
// SQL escaping
// ============================================================

function esc(v: string | null | undefined): string {
  if (v === null || v === undefined) return 'NULL';
  return "'" + v.replace(/'/g, "''") + "'";
}

function escJson(v: unknown): string {
  if (v === null || v === undefined) return 'NULL';
  return "'" + JSON.stringify(v).replace(/'/g, "''") + "'::jsonb";
}

function escNum(v: number | null | undefined): string {
  if (v === null || v === undefined) return 'NULL';
  return String(v);
}

function escBool(v: boolean): string {
  return v ? 'true' : 'false';
}

function escTs(v: string | Date | null | undefined): string {
  if (!v) return 'NULL';
  const d = typeof v === 'string' ? v : v.toISOString();
  return `'${d}'::timestamptz`;
}

// ============================================================
// Main
// ============================================================

async function main() {
  const tmpDir = process.env.TEMP || process.env.TMP || '/tmp';
  const inputPath = `${tmpDir}/vector2_objects.jsonl.gz`;

  if (!fs.existsSync(inputPath)) {
    console.error(`Input file not found: ${inputPath}`);
    console.error('Download from vec.atlanta.ua first.');
    process.exit(1);
  }

  console.log('=== Vector2 Object Import ===\n');

  // Connect to local DB
  const client = new Client({
    host: 'localhost', port: 5433, user: 'postgres', password: 'postgres', database: 'valuation',
  });
  await client.connect();
  console.log('Connected to local DB');

  // Load source_id_mappings
  console.log('Loading source_id_mappings...');
  const geoMap = new Map<number, number>();
  const streetMap = new Map<number, number>();
  const complexMap = new Map<number, number>();

  const mappingRes = await client.query(
    `SELECT entity_type, source_id, local_id FROM source_id_mappings WHERE source = 'vector2_crm'`,
  );
  for (const row of mappingRes.rows) {
    const sourceId = row.source_id;
    const localId = row.local_id;
    switch (row.entity_type) {
      case 'geo': geoMap.set(sourceId, localId); break;
      case 'street': streetMap.set(sourceId, localId); break;
      case 'complex': complexMap.set(sourceId, localId); break;
    }
  }
  console.log(`  Geo mappings: ${geoMap.size}`);
  console.log(`  Street mappings: ${streetMap.size}`);
  console.log(`  Complex mappings: ${complexMap.size}`);

  // Count existing vector_crm records
  const existingRes = await client.query(
    `SELECT count(*) FROM unified_listings WHERE source_type = 'vector_crm'`,
  );
  console.log(`  Existing vector_crm listings: ${existingRes.rows[0].count}`);

  // Stats
  let total = 0;
  let inserted = 0;
  let skippedNoGeo = 0;
  let errors = 0;
  const batchSize = 500;
  let batch: string[] = [];

  const columns = [
    'source_type', 'source_id', 'source_global_id',
    'deal_type', 'realty_type',
    'geo_id', 'street_id', 'topzone_id', 'complex_id',
    'lat', 'lng',
    'price', 'currency', 'price_per_meter',
    'total_area', 'living_area', 'kitchen_area', 'land_area',
    'rooms', 'floor', 'total_floors',
    'condition', 'house_type', 'planning_type', 'heating_type',
    'attributes', 'infrastructure',
    'nearest_school', 'nearest_hospital', 'nearest_supermarket',
    'nearest_parking', 'nearest_public_transport',
    'is_active', 'is_exclusive',
    'published_at', 'synced_at',
    'realty_platform',
  ];

  async function flushBatch() {
    if (batch.length === 0) return;
    const sql = `INSERT INTO unified_listings (${columns.join(', ')})
VALUES ${batch.join(',\n')}
ON CONFLICT (source_type, source_id) DO UPDATE SET
  deal_type = EXCLUDED.deal_type,
  realty_type = EXCLUDED.realty_type,
  geo_id = EXCLUDED.geo_id,
  street_id = EXCLUDED.street_id,
  topzone_id = EXCLUDED.topzone_id,
  complex_id = EXCLUDED.complex_id,
  lat = EXCLUDED.lat,
  lng = EXCLUDED.lng,
  price = EXCLUDED.price,
  currency = EXCLUDED.currency,
  price_per_meter = EXCLUDED.price_per_meter,
  total_area = EXCLUDED.total_area,
  living_area = EXCLUDED.living_area,
  kitchen_area = EXCLUDED.kitchen_area,
  land_area = EXCLUDED.land_area,
  rooms = EXCLUDED.rooms,
  floor = EXCLUDED.floor,
  total_floors = EXCLUDED.total_floors,
  condition = EXCLUDED.condition,
  house_type = EXCLUDED.house_type,
  planning_type = EXCLUDED.planning_type,
  heating_type = EXCLUDED.heating_type,
  attributes = EXCLUDED.attributes,
  infrastructure = EXCLUDED.infrastructure,
  nearest_school = EXCLUDED.nearest_school,
  nearest_hospital = EXCLUDED.nearest_hospital,
  nearest_supermarket = EXCLUDED.nearest_supermarket,
  nearest_parking = EXCLUDED.nearest_parking,
  nearest_public_transport = EXCLUDED.nearest_public_transport,
  is_active = EXCLUDED.is_active,
  is_exclusive = EXCLUDED.is_exclusive,
  published_at = EXCLUDED.published_at,
  synced_at = EXCLUDED.synced_at,
  realty_platform = EXCLUDED.realty_platform`;
    try {
      await client.query(sql);
      inserted += batch.length;
    } catch (err: any) {
      console.error(`Batch insert error at row ~${total}:`, err.message);
      errors += batch.length;
    }
    batch = [];
  }

  // Process JSONL
  console.log('\nProcessing objects...');
  const startTime = Date.now();

  const gunzip = zlib.createGunzip();
  const stream = fs.createReadStream(inputPath).pipe(gunzip);
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  for await (const line of rl) {
    total++;
    if (total % 50000 === 0) {
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
      console.log(`  ${total} rows processed (${elapsed}s), inserted=${inserted}, errors=${errors}`);
    }

    let row: Vector2Row;
    try {
      row = JSON.parse(line);
    } catch {
      errors++;
      continue;
    }

    const attrs = row.attributes_data || {};
    const realtyType = mapRealtyType(row.type_estate, row.fk_subcatid, attrs);
    const dealType = mapDealType(row);
    const price = getPrice(row);
    const totalArea = num(row.square_total ?? attrs.square_total);
    const livingArea = num(row.square_living ?? attrs.square_living);
    const kitchenArea = num(attrs.square_kitchen);
    const landArea = num(row.square_land_total ?? attrs.square_land_total);
    const lat = num(row.map_x);
    const lng = num(row.map_y);
    const rooms = int(attrs.rooms_count);
    const floor = int(attrs.floor);
    const totalFloors = int(attrs.floors_count);
    const condition = mapDict(attrs.condition_type, CONDITION_MAP);
    const houseType = mapDict(attrs.project, PROJECT_MAP) || mapDict(attrs.housing_material, MATERIAL_MAP);
    const planningType = mapDict(attrs.location_rooms, PLANNING_MAP);
    const heatingType = mapDict(attrs.heating_type, HEATING_MAP);
    const currency = extractCurrency(row, attrs);
    const pricePerMeter = totalArea && price ? price / totalArea : null;
    const isActive = !row.is_archive;
    const isExclusive = row.is_exclusive === 1 || row.is_exclusive === true;

    // Resolve IDs via source_id_mappings
    const geoId = geoMap.get(row.fk_geo_id) ?? null;
    const streetId = row.geo_street ? (streetMap.get(row.geo_street) ?? null) : null;
    const complexId = int(attrs.geo_zk) ? (complexMap.get(int(attrs.geo_zk)!) ?? null) : null;

    // Extract infrastructure distances
    const distances = extractNearestDistances(row.nearest_infrastructure);

    // Sanitize rooms/floor/totalFloors from crazy values (known issue)
    const safeRooms = rooms !== null && rooms >= 1 && rooms <= 50 ? rooms : null;
    const safeFloor = floor !== null && floor >= -5 && floor <= 200 ? floor : null;
    const safeTotalFloors = totalFloors !== null && totalFloors >= 1 && totalFloors <= 200 ? totalFloors : null;

    const values = `(
  'vector_crm', ${escNum(row.id)}, ${esc(row.global_id)},
  ${esc(dealType)}, ${esc(realtyType)},
  ${escNum(geoId)}, ${escNum(streetId)}, NULL, ${escNum(complexId)},
  ${escNum(lat)}, ${escNum(lng)},
  ${escNum(price)}, ${esc(currency)}, ${escNum(pricePerMeter)},
  ${escNum(totalArea)}, ${escNum(livingArea)}, ${escNum(kitchenArea)}, ${escNum(landArea)},
  ${escNum(safeRooms)}, ${escNum(safeFloor)}, ${escNum(safeTotalFloors)},
  ${esc(condition)}, ${esc(houseType)}, ${esc(planningType)}, ${esc(heatingType)},
  ${escJson(attrs)}, ${escJson(Array.isArray(row.nearest_infrastructure) ? row.nearest_infrastructure : null)},
  ${escNum(distances.nearest_school ?? null)}, ${escNum(distances.nearest_hospital ?? null)},
  ${escNum(distances.nearest_supermarket ?? null)}, ${escNum(distances.nearest_parking ?? null)},
  ${escNum(distances.nearest_public_transport ?? null)},
  ${escBool(isActive)}, ${escBool(isExclusive)},
  ${escTs(row.time_create)}, NOW(),
  'vector_crm'
)`;

    batch.push(values);
    if (batch.length >= batchSize) {
      await flushBatch();
    }
  }

  // Flush remaining
  await flushBatch();

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`\n=== DONE ===`);
  console.log(`Total rows: ${total}`);
  console.log(`Inserted/updated: ${inserted}`);
  console.log(`Skipped (no geo): ${skippedNoGeo}`);
  console.log(`Errors: ${errors}`);
  console.log(`Time: ${elapsed}s`);

  // Final counts
  const countRes = await client.query(`
    SELECT
      count(*) as total,
      count(*) FILTER (WHERE source_type = 'vector_crm') as vector_crm,
      count(*) FILTER (WHERE source_type = 'vector') as vector,
      count(*) FILTER (WHERE source_type = 'aggregator') as aggregator
    FROM unified_listings
  `);
  console.log('\nUnified listings by source:');
  for (const row of countRes.rows) {
    console.log(`  vector_crm: ${row.vector_crm}, vector: ${row.vector}, aggregator: ${row.aggregator}, TOTAL: ${row.total}`);
  }

  // Geo resolution stats for vector_crm
  const geoStatsRes = await client.query(`
    SELECT
      count(*) as total,
      count(*) FILTER (WHERE geo_id IS NOT NULL) as has_geo,
      count(*) FILTER (WHERE street_id IS NOT NULL) as has_street,
      count(*) FILTER (WHERE complex_id IS NOT NULL) as has_complex,
      count(*) FILTER (WHERE price IS NOT NULL) as has_price,
      count(*) FILTER (WHERE is_active) as active
    FROM unified_listings WHERE source_type = 'vector_crm'
  `);
  console.log('\nVector_crm stats:');
  const s = geoStatsRes.rows[0];
  console.log(`  Total: ${s.total}, Active: ${s.active}`);
  console.log(`  Has geo: ${s.has_geo} (${(100 * s.has_geo / s.total).toFixed(1)}%)`);
  console.log(`  Has street: ${s.has_street} (${(100 * s.has_street / s.total).toFixed(1)}%)`);
  console.log(`  Has complex: ${s.has_complex} (${(100 * s.has_complex / s.total).toFixed(1)}%)`);
  console.log(`  Has price: ${s.has_price} (${(100 * s.has_price / s.total).toFixed(1)}%)`);

  await client.end();
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
