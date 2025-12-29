import { NestFactory } from '@nestjs/core';

import { StreetOsmParserService } from '../apps/valuation/src/modules/osm/street-osm-parser.service';

import { ParseStreetsOsmModule } from './parse-streets-osm.module';

const main = async (): Promise<void> => {
  const app = await NestFactory.createApplicationContext(ParseStreetsOsmModule, {
    logger: ['log', 'error', 'warn'],
  });
  const streetOsmParserService = app.get(StreetOsmParserService);

  const args = process.argv.slice(2);
  const command = args[0];

  try {
    if (command === 'list') {
      console.log('Available regions:');
      const regions = await streetOsmParserService.getAvailableRegions();

      if (regions.length === 0) {
        console.log('No regions found. Run parse-geo-osm first to populate geo data.');
      } else {
        regions.forEach((region) => {
          console.log(`  ${region.alias.padEnd(20)} - ${region.name} (ID: ${region.id})`);
        });
      }
    } else if (command === 'region' && args[1]) {
      const alias = args[1].toLowerCase();
      const region = await streetOsmParserService.findRegionByAlias(alias);

      if (!region) {
        console.log(`Region not found: ${alias}`);
        console.log('Use "yarn parse-streets-osm list" to see available regions');
        process.exit(1);
      }

      console.log(`Parsing streets for ${region.name}...`);
      const result = await streetOsmParserService.parseRegionStreets(region.id);
      console.log(`\nSummary:`);
      console.log(`  Settlements processed: ${result.settlementsProcessed}`);
      console.log(`  Settlements failed: ${result.settlementsFailed}`);
      console.log(`  Total streets: ${result.totalStreets}`);
      console.log(`  Errors: ${result.totalErrors}`);
    } else if (command === 'settlement' && args[1]) {
      const geoId = parseInt(args[1], 10);

      if (isNaN(geoId)) {
        console.log('Invalid settlement ID. Must be a number.');
        process.exit(1);
      }

      console.log(`Parsing streets for settlement ID: ${geoId}...`);
      const result = await streetOsmParserService.parseSettlementStreets(geoId);
      console.log(`Result: ${result.streetsCount} streets, ${result.errorCount} errors`);
    } else if (command === 'settlements' && args[1]) {
      const alias = args[1].toLowerCase();
      const region = await streetOsmParserService.findRegionByAlias(alias);

      if (!region) {
        console.log(`Region not found: ${alias}`);
        process.exit(1);
      }

      console.log(`Settlements in ${region.name}:`);
      const settlements = await streetOsmParserService.getSettlementsInRegion(region.id);
      settlements.forEach((s) => {
        console.log(`  ${s.id.toString().padEnd(8)} - ${s.name} (${s.type})`);
      });
      console.log(`\nTotal: ${settlements.length} settlements`);
    } else {
      console.log('Usage:');
      console.log('  yarn parse-streets-osm list                    - List available regions');
      console.log('  yarn parse-streets-osm region <alias>          - Parse all streets in region');
      console.log('  yarn parse-streets-osm settlement <id>         - Parse streets for single settlement');
      console.log('  yarn parse-streets-osm settlements <alias>     - List settlements in region');
      console.log('');
      console.log('Examples:');
      console.log('  yarn parse-streets-osm region odeska-oblast');
      console.log('  yarn parse-streets-osm settlement 12345');
    }
  } catch (error) {
    console.error('Error:', (error as Error).message);
  } finally {
    await app.close();
    process.exit(0);
  }
};

void main();
