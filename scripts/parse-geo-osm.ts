import { NestFactory } from '@nestjs/core';

import { GeoOsmParserService } from '../apps/valuation/src/modules/osm/geo-osm-parser.service';

import { ParseGeoOsmModule } from './parse-geo-osm.module';

const REGIONS: Record<string, { osmId: number; name: string }> = {
  odesa: { osmId: 72634, name: 'Одеська область' },
  kyiv: { osmId: 71248, name: 'Київська область' },
  lviv: { osmId: 72380, name: 'Львівська область' },
  kharkiv: { osmId: 71254, name: 'Харківська область' },
  dnipro: { osmId: 101746, name: 'Дніпропетровська область' },
  vinnytsia: { osmId: 90726, name: 'Вінницька область' },
  zakarpattia: { osmId: 72489, name: 'Закарпатська область' },
  ivanofrankivsk: { osmId: 72488, name: 'Івано-Франківська область' },
  chernivtsi: { osmId: 72526, name: 'Чернівецька область' },
  cherkasy: { osmId: 91278, name: 'Черкаська область' },
  chernihiv: { osmId: 71249, name: 'Чернігівська область' },
  khmelnytskyi: { osmId: 90742, name: 'Хмельницька область' },
  kirovohrad: { osmId: 101859, name: 'Кіровоградська область' },
  mykolaiv: { osmId: 72635, name: 'Миколаївська область' },
  poltava: { osmId: 91294, name: 'Полтавська область' },
  rivne: { osmId: 71236, name: 'Рівненська область' },
  sumy: { osmId: 71250, name: 'Сумська область' },
  ternopil: { osmId: 72525, name: 'Тернопільська область' },
  volyn: { osmId: 71064, name: 'Волинська область' },
  zaporizhzhia: { osmId: 71980, name: 'Запорізька область' },
  zhytomyr: { osmId: 71245, name: 'Житомирська область' },
  donetsk: { osmId: 71973, name: 'Донецька область' },
  luhansk: { osmId: 71971, name: 'Луганська область' },
  kherson: { osmId: 71022, name: 'Херсонська область' },
  kyivcity: { osmId: 421866, name: 'Київ (місто)' },
};

const main = async (): Promise<void> => {
  const app = await NestFactory.createApplicationContext(ParseGeoOsmModule, {
    logger: ['log', 'error', 'warn'],
  });
  const geoOsmParserService = app.get(GeoOsmParserService);

  const args = process.argv.slice(2);
  const command = args[0];

  try {
    if (command === 'region' && args[1]) {
      const regionKey = args[1].toLowerCase();
      const region = REGIONS[regionKey];

      if (!region) {
        console.log(`Unknown region: ${args[1]}`);
        console.log(`Available regions: ${Object.keys(REGIONS).join(', ')}`);
        process.exit(1);
      }

      console.log(`Parsing ${region.name} (OSM ID: ${region.osmId})...`);
      const result = await geoOsmParserService.parseRegion(region.osmId);
      console.log(`Result: ${result.total} records, ${result.errors} errors`);
    } else if (command === 'osm' && args[1]) {
      const osmId = parseInt(args[1], 10);
      console.log(`Parsing region with OSM ID: ${osmId}...`);
      const result = await geoOsmParserService.parseRegion(osmId);
      console.log(`Result: ${result.total} records, ${result.errors} errors`);
    } else if (command === 'all') {
      console.log('Parsing all regions...\n');

      for (const [key, region] of Object.entries(REGIONS)) {
        try {
          console.log(`\n${'='.repeat(60)}`);
          console.log(`Parsing ${region.name} (${key})...`);
          const result = await geoOsmParserService.parseRegion(region.osmId, true);
          console.log(`Result: ${result.total} records, ${result.errors} errors`);
        } catch (error) {
          console.error(`Error parsing ${region.name}: ${(error as Error).message}`);
        }

        await new Promise((resolve) => setTimeout(resolve, 5000));
      }

      console.log('\n' + '='.repeat(60));
      console.log('Rebuilding nested set for all regions...');
      await geoOsmParserService.rebuildNestedSet();
      console.log('Done!');
    } else {
      console.log('Usage:');
      console.log('  yarn parse-geo-osm region <name>  - Parse region by name');
      console.log('  yarn parse-geo-osm osm <osmId>    - Parse region by OSM relation ID');
      console.log('  yarn parse-geo-osm all            - Parse all Ukrainian regions');
      console.log('');
      console.log('Available regions:');
      Object.entries(REGIONS).forEach(([key, value]) => {
        console.log(`  ${key.padEnd(18)} - ${value.name} (OSM: ${value.osmId})`);
      });
    }
  } catch (error) {
    console.error('Error:', (error as Error).message);
  } finally {
    await app.close();
    process.exit(0);
  }
};

void main();
