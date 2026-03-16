const fs = require('fs');

// CRM type -> our type mapping
const CRM_TYPE_MAP = { 3: 'city', 4: 'city_district', 6: 'village' };

// Load our geos with region: id|uk|ru|type|region_uk
const ourGeos = [];
for (const line of fs.readFileSync('C:/Users/doonly/our_geos_with_region.csv', 'utf8').trim().split('\n')) {
  const parts = line.split('|');
  if (parts.length < 5) continue;
  ourGeos.push({
    id: parts[0].trim(),
    uk: (parts[1] || '').trim(),
    ru: (parts[2] || '').trim(),
    type: (parts[3] || '').trim(),
    region: (parts[4] || '').trim(),
  });
}
console.log('Our geos loaded:', ourGeos.length);

// Unmapped CRM geos with region info
// [crmId, crmName, crmType, parentRegion, parentCity (if district)]
const unmapped = [
  [95, 'Усатово Ст.', 6, 'Одеська область', null],
  [193, 'Южний', 3, 'Одеська область', null],
  [172, 'Первомайське', 6, 'Одеська область', null],
  [106, 'Коблево', 6, 'Одеська область', null],
  [269, 'Палієво', 6, 'Одеська область', null],
  [918, 'Чорноморка', 6, 'Миколаївська область', null],
  [141, 'Гвардійське', 6, 'Одеська область', null],
  [561, 'Приднепровский район', 4, 'Черкаська область', 'Черкаси'],
  [560, 'Сосновский район', 4, 'Черкаська область', 'Черкаси'],
  [210, 'Гребний Канал', 6, 'Одеська область', null],
  [85, 'Нати', 6, 'Одеська область', null],
  [114, 'Рибаковка', 6, 'Одеська область', null],
  [101, 'Червоний Расселенець', 6, 'Одеська область', null],
  [3599, 'Борислав', 3, 'Львівська область', null],
  [3261, 'Тульчин', 3, 'Вінницька область', null],
  [190, 'Широка Нива', 6, 'Одеська область', null],
  [3586, 'Оріховиця', 6, 'Закарпатська область', null],
  [206, 'Бугово', 6, 'Одеська область', null],
  [2984, 'Стадниця', 6, 'Вінницька область', null],
  [278, 'Фрунзівка', 6, 'Одеська область', null],
  [3605, 'Худльово', 6, 'Закарпатська область', null],
  [3248, 'Ладижин', 3, 'Вінницька область', null],
  [3564, 'Тересва', 6, 'Закарпатська область', null],
  [710, 'Богунський район', 4, 'Житомирська область', 'Житомир'],
  [3604, 'Великі Глібовичі', 6, 'Львівська область', null],
  [3606, 'Тисауйфалу', 6, 'Закарпатська область', null],
  [553, 'Інгульський район', 4, 'Миколаївська область', 'Миколаїв'],
  [714, 'Покровський район', 4, 'Дніпропетровська область', 'Кривий Ріг'],
  [569, 'Червона Слобода', 6, 'Черкаська область', null],
  [716, 'Центрально-Міський район', 4, 'Дніпропетровська область', 'Кривий Ріг'],
  [3603, 'Завосина', 6, 'Закарпатська область', null],
  [717, 'Довгинцівський район', 4, 'Дніпропетровська область', 'Кривий Ріг'],
  [718, 'Металургійний район', 4, 'Дніпропетровська область', 'Кривий Ріг'],
  [520, 'Опитне', 6, 'Одеська область', null],
  [955, 'Покровське', 6, 'Миколаївська область', null],
  [75, 'Красна Гірка', 6, 'Одеська область', null],
  [665, 'Дніпровський район', 4, 'Дніпропетровська область', "Кам'янське"],
];

function normalize(s) {
  return s.toLowerCase()
    .replace(/[''ʼ`]/g, "'")
    .replace(/\s+/g, ' ')
    .replace(/\bст\.\s*/g, '')
    .replace(/\bсмт\.\s*/g, '')
    .trim();
}

function regionMatch(ourRegion, crmRegion) {
  if (!ourRegion || !crmRegion) return true; // no filter
  const a = normalize(ourRegion);
  const b = normalize(crmRegion);
  // Substring match for region
  return a.includes(b.slice(0, 6)) || b.includes(a.slice(0, 6));
}

console.log('');
console.log('CRM_ID  CRM_Name                          Region                     -> Our_ID  Our_Name                       Our_Region');
console.log('-'.repeat(140));

const results = [];
const notFound = [];

for (const [crmId, crmName, crmType, crmRegion, crmCity] of unmapped) {
  const crmNorm = normalize(crmName);
  const ourType = CRM_TYPE_MAP[crmType];

  let best = null;

  for (const g of ourGeos) {
    // Region filter - MUST match region to avoid false positives
    if (!regionMatch(g.region, crmRegion)) continue;

    const guk = normalize(g.uk);
    const gru = normalize(g.ru);

    // Exact match (with normalization)
    if (guk === crmNorm || gru === crmNorm) {
      const typeMatch = (g.type === ourType) ||
        (g.type === 'village' && ourType === 'village') ||
        (g.type === 'city' && ourType === 'city');
      const score = typeMatch ? 1.0 : 0.95;
      if (!best || score > best.score) {
        best = { ...g, score, method: 'exact_name' };
      }
    }

    // Substring match (for longer names)
    if (!best && crmNorm.length >= 5) {
      if (guk.includes(crmNorm) || gru.includes(crmNorm)) {
        const score = 0.85;
        if (!best || score > best.score) {
          best = { ...g, score, method: 'substring' };
        }
      }
      if (crmNorm.includes(guk) && guk.length >= 5) {
        const score = 0.80;
        if (!best || score > best.score) {
          best = { ...g, score, method: 'substring' };
        }
      }
    }
  }

  if (best) {
    console.log(
      String(crmId).padStart(6) + '  ' + crmName.padEnd(30) + ' ' + crmRegion.padEnd(25) +
      ' -> ' + best.id.padStart(6) + '  ' + (best.uk || best.ru).padEnd(30) + ' ' + best.region
    );
    results.push({ crmId, localId: best.id, score: best.score, method: best.method, crmName, ourName: best.uk || best.ru });
  } else {
    console.log(String(crmId).padStart(6) + '  ' + crmName.padEnd(30) + ' ' + crmRegion.padEnd(25) + ' -> ???    NOT FOUND');
    notFound.push({ crmId, crmName, crmType, crmRegion });
  }
}

console.log('\nMatched:', results.length, '/', unmapped.length);
console.log('Not found:', notFound.length);

if (notFound.length > 0) {
  console.log('\nNot found:');
  for (const nf of notFound) {
    console.log(`  CRM ${nf.crmId}: "${nf.crmName}" (${nf.crmRegion})`);
  }
}

if (results.length > 0) {
  console.log('\n-- SQL to insert mappings:');
  for (const r of results) {
    console.log(
      `INSERT INTO source_id_mappings (source, entity_type, source_id, local_id, confidence, match_method, created_at) ` +
      `VALUES ('vector2_crm', 'geo', ${r.crmId}, ${r.localId}, ${r.score.toFixed(2)}, '${r.method}', NOW()) ` +
      `ON CONFLICT DO NOTHING; -- ${r.crmName} -> ${r.ourName}`
    );
  }
}
