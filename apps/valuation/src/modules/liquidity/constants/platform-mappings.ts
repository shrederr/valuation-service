/**
 * Маппінг-константи для парсингу primaryData з різних платформ.
 *
 * Платформи (по полю realtyPlatform):
 *   olx (482k) — params: [{key, value}] масив
 *   domRia (274k) — characteristics_values: {id: value}, wall_type, wall_type_uk
 *   realtorUa (263k) — main_params: {}, addition_params: []
 *   realEstateLvivUa (72k) — details: {"Ключ": "значення"}
 *   mlsUkraine (5k) — params: {key: value} об'єкт
 *   vector_crm (394k) — немає primaryData, є attributes
 *   unknown (27k) — різне
 */

// ─── Condition / Стан ────────────────────────────────────────────────

/** domRia: characteristics_values['516'] → {text, score} */
export const DOMRIA_CONDITION_MAP: Record<string, { text: string; score: number }> = {
  '506': { text: 'Дизайнерський ремонт', score: 10 },
  '1885': { text: 'Авторський проєкт', score: 10 },
  '507': { text: 'Євроремонт', score: 9 },
  '508': { text: 'Хороший стан', score: 7 },
  '510': { text: 'Задовільний стан', score: 6 },
  '509': { text: 'Косметичний ремонт', score: 5 },
  '513': { text: 'Після будівельників', score: 3 },
  '512': { text: 'Чорнова штукатурка', score: 2 },
  '515': { text: 'Під чистову обробку', score: 2 },
  '511': { text: 'Потребує ремонту', score: 1 },
  '514': { text: 'Аварійний стан', score: 0 },
};

/** realtorUa: main_params.status → score */
export const REALTOR_CONDITION_SCORE: Record<string, number> = {
  'дизайнерський ремонт': 10,
  'авторський проект': 10,
  'євроремонт': 9,
  'чудовий стан': 8,
  'хороший стан': 7,
  'з ремонтом': 7,
  'задовільний стан': 6,
  'частковий ремонт': 5,
  'косметичний ремонт': 5,
  'після будівельників': 3,
  'чорнова штукатурка': 2,
  'без ремонту': 1,
  'потребує ремонту': 1,
  'потрібен капітальний ремонт': 0,
};

/** realEstateLvivUa: details["Стан"] → score */
export const REAL_ESTATE_CONDITION_SCORE: Record<string, number> = {
  'люкс': 10,
  'відмінний': 9,
  'євроремонт': 9,
  'хороший': 7,
  'задовільний': 5,
  'потребує ремонту': 1,
  'без ремонту': 1,
  'аварійний': 0,
};

/** vector_crm: attributes.condition_type → {text, score} */
/**
 * Vector2 CRM condition_type codes → liquidity scoring
 * Source of truth: libs/common VECTOR2_CONDITION_TYPE_MAP
 */
export const VECTOR_CONDITION_TYPE_MAP: Record<number, { text: string; score: number }> = {
  // Vector2 codes (1-24)
  1: { text: 'Потрібен капітальний ремонт', score: 0 },
  2: { text: 'Потрібен поточний ремонт', score: 1 },
  3: { text: 'Потрібен косметичний ремонт', score: 1 },
  4: { text: 'Після капремонту', score: 5 },
  5: { text: 'Євроремонт', score: 9 },
  6: { text: 'Будинок, що будується', score: 2 },
  7: { text: 'Після будівельників', score: 3 },
  8: { text: 'Після пожежі', score: 0 },
  9: { text: 'Після повені', score: 0 },
  10: { text: 'Стіни сирі', score: 0 },
  11: { text: 'Під знос', score: 0 },
  12: { text: 'Недобудоване', score: 1 },
  13: { text: 'Нуль цикл', score: 1 },
  14: { text: 'Будматеріали', score: 1 },
  15: { text: 'Тільки документи', score: 0 },
  16: { text: 'Дах потрібний ремонт', score: 1 },
  17: { text: 'Потріб. капрем. та дах', score: 0 },
  18: { text: 'Потріб. тек. рем. та дах', score: 1 },
  19: { text: 'Потріб. космет. рем. та дах', score: 1 },
  20: { text: 'Житлове чисте', score: 7 },
  21: { text: 'Після косметики', score: 5 },
  22: { text: 'Ремонт не потрібний', score: 7 },
  24: { text: 'White Box', score: 2 },
  // Legacy/extended codes (from older vector CRM data)
  69: { text: 'Хороший стан', score: 7 },
  81: { text: 'Євроремонт', score: 9 },
  97: { text: 'Косметичний ремонт', score: 5 },
  99: { text: 'Хороший стан', score: 7 },
  100: { text: 'Авторський проект', score: 10 },
  167: { text: 'Після будівельників', score: 3 },
  169: { text: 'Після будівельників', score: 3 },
  173: { text: 'Житловий стан', score: 7 },
  458: { text: 'Євроремонт', score: 9 },
  783: { text: 'Під чистову обробку', score: 2 },
  800: { text: 'Під чистову обробку', score: 2 },
};

// ─── House Type / Тип будинку ────────────────────────────────────────

/** domRia: wall_type / wall_type_uk → score */
export const WALL_TYPE_SCORE: Record<string, number> = {
  // Моноліт (10)
  'моноліт': 10, 'монолит': 10,
  'монолітно-каркасний': 10, 'монолитно-каркасный': 10,
  'монолітно-цегляний': 10, 'монолитно-кирпичный': 10,
  'монолітно-блоковий': 10, 'монолитно-блочный': 10,
  // Цегла (8)
  'цегла': 8, 'кирпич': 8,
  'силікатна цегла': 8, 'силикатный кирпич': 8,
  // Ракушняк (7)
  'ракушняк': 7, 'ракушечник (ракушняк)': 7, 'ракушечник': 7,
  // Каркас (7)
  'каркасний': 7, 'каркасный': 7,
  // Блок (6)
  'газоблок': 6, 'газобетон': 6,
  'піноблок': 6, 'пеноблок': 6,
  'керамічний блок': 6, 'керамический блок': 6,
  'керамзітобетон': 6, 'керамзитобетон': 6,
  'шлакоблок': 6,
  // Панель (4)
  'панель': 4, 'панельний': 4, 'панельный': 4,
  'залізобетон': 4, 'железобетон': 4,
};

// ─── Layout / Планування ─────────────────────────────────────────────

/** Тип планування → score */
export const LAYOUT_TYPE_SCORE: Record<string, number> = {
  'студія': 10, 'студио': 10, 'studio': 10,
  'пентхаус': 10, 'penthouse': 10,
  'кухня-вітальня': 9, 'кухня-гостиная': 9,
  'багаторівнева': 8, 'многоуровневая': 8, 'дворівнева': 8, 'двухуровневая': 8,
  'роздільне': 8, 'раздельная': 8, 'роздільна': 8,
  'суміжно-роздільне': 6, 'смежно-раздельная': 6, 'суміжно-роздільна': 6,
  'суміжне': 4, 'смежная': 4, 'суміжна': 4,
};

// ─── Comfort / Комфорт — нормалізація OLX тегів ──────────────────────

export const OLX_COMFORT_NORMALIZE: Record<string, string> = {
  'балкон': 'balcony',
  'лоджія': 'loggia',
  'лоджия': 'loggia',
  'ліфт': 'elevator',
  'лифт': 'elevator',
  'кондиціонер': 'conditioner',
  'кондиционер': 'conditioner',
  'гараж': 'garage',
  'паркінг': 'parking',
  'парковка': 'parking',
  'панорамні вікна': 'panoramic_windows',
  'панорамные окна': 'panoramic_windows',
  'камін': 'fireplace',
  'камин': 'fireplace',
  'тераса': 'terrace',
  'терраса': 'terrace',
  'басейн': 'pool',
  'бассейн': 'pool',
  'сауна': 'sauna',
  'закрита територія': 'closed_area',
  'закрытая территория': 'closed_area',
  'охорона': 'security',
  'охрана': 'security',
  'відеонагляд': 'cctv',
  'видеонаблюдение': 'cctv',
  'домофон': 'intercom',
  'консьєрж': 'concierge',
  'консьерж': 'concierge',
};
