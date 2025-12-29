const fs = require('fs');
const content = fs.readFileSync('scripts/process-aggregator-dump.ts', 'utf8');

const oldCode = `function mapOlxCondition(params: unknown): string | undefined {
  // Спочатку перевіряємо repair (для житлової нерухомості)
  const repair = getOlxParamValue(params, 'repair');
  if (!repair) return undefined;
  const result = OLX_REPAIR_MAP[repair];
  if (!result && repair) {
    unknownOlxRepairs.set(repair, (unknownOlxRepairs.get(repair) || 0) + 1);
  }
  return result;
}`;

const newCode = `function mapOlxCondition(params: unknown): string | undefined {
  // Спочатку перевіряємо 'repair' (для житлової нерухомості)
  const repair = getOlxParamValue(params, 'repair');
  if (repair) {
    const result = OLX_REPAIR_MAP[repair];
    if (!result) {
      unknownOlxRepairs.set(repair, (unknownOlxRepairs.get(repair) || 0) + 1);
    }
    return result;
  }

  // Для комерційної нерухомості перевіряємо 'is_repaired'
  const isRepaired = getOlxParamValue(params, 'is_repaired');
  if (isRepaired) {
    if (isRepaired === 'Так' || isRepaired === 'yes') {
      return 'Євроремонт';
    }
    if (isRepaired === 'Ні' || isRepaired === 'no') {
      return 'Без ремонту';
    }
  }

  return undefined;
}`;

if (content.includes(oldCode)) {
  const updated = content.replace(oldCode, newCode);
  fs.writeFileSync('scripts/process-aggregator-dump.ts', updated);
  console.log('Updated successfully!');
} else {
  console.log('Old code not found, showing current:');
  const match = content.match(/function mapOlxCondition[\s\S]*?^}/m);
  if (match) console.log(match[0]);
}
