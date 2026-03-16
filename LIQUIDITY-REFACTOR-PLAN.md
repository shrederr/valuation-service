# Plan: Рефакторинг системи оцінки ліквідності

> Дата: 2026-02-23
> Статус: Затверджено до реалізації

---

## 0. Контекст

### Платформи в БД (серверна)
| Платформа | Записів | source_type | Формат primaryData |
|-----------|---------|-------------|-------------------|
| olx | 482,056 | aggregator | `params: [{key, value}]` масив |
| vector_crm | 393,605 | vector_crm | немає primaryData, є `attributes` |
| domRia | 273,493 | aggregator | прямі поля + `characteristics_values: {id: value}` |
| realtorUa | 262,861 | aggregator | `main_params: {}` + `addition_params: []` |
| realEstateLvivUa | 72,190 | aggregator | `details: {"Ключ": "значення"}` |
| unknown | 26,994 | aggregator | різне |
| mlsUkraine | 4,882 | aggregator | `params: {key: value}` об'єкт |

### ТЗ
Файл: `D:\analogis\Формулы расчёта баллов и итоговой ликвидності.md`

---

## 1. Архітектурні зміни

### 1.1. PrimaryDataExtractor — визначення платформи по `realtyPlatform`

**ПОМИЛКА:** Зараз платформа визначається по структурі JSON (Array.isArray(params) → OLX, main_params → "domRia", etc.)
**ФАКТ:** В entity `UnifiedListing` є поле `realtyPlatform` (olx | domRia | realtorUa | realEstateLvivUa | mlsUkraine | vector_crm | unknown), яке заповнюється при синхронізації.

**FIX:** Додати метод `getPlatform(listing): string` в PrimaryDataExtractor:
```typescript
private getPlatform(listing: UnifiedListingEntity): string {
  return listing.realtyPlatform || 'unknown';
}
```
Кожен extract-метод повинен робити `switch(this.getPlatform(listing))` замість угадування по JSON.

### 1.2. domRia перепутан з realtorUa

**ПОМИЛКА:** Методи `extractDomRiaMainParam()` парсять `main_params.status`, `main_params.border` — це формат **realtorUa**, НЕ domRia.
- domRia: `characteristics_values`, `wall_type`, `wall_type_uk`, `description_uk`
- realtorUa: `main_params.status`, `main_params.border`, `main_params.planirovka`, `addition_params`

**FIX:** Перейменувати/розділити методи. Кожен має чітко відповідати своїй платформі.

### 1.3. Дублювання з AttributeMapperService

**ФАКТ:** При синхронізації `AttributeMapperService` вже правильно маппить `condition` і `houseType` в entity-колонки для всіх платформ.
**ВИСНОВОК:** Для `condition` і `houseType` — entity колонки як primary source. PrimaryDataExtractor як fallback тільки якщо entity порожній.

---

## 2. Ваги критеріїв — перерозподіл

### Критерії що ВИДАЛЯЄМО (немає даних):
- **Вид з вікон** (W=0.04) — немає даних на жодній платформі
- **Природа** (W=0.03) — мало даних, ненадійно
- **Тип вікон** (W=0.05) — немає даних на жодній платформі

Сума звільнених ваг: **0.12**

### Критерії що ЗАЛИШАЄМО (13 штук):

Звільнено 0.12 ваги від 3 видалених критеріїв. Розподіл: пропорційно важливості.

| # | Критерій | Стара вага (ТЗ) | Нова вага | Зміна |
|---|----------|----------------|-----------|-------|
| 1 | Ціна | 0.20 | 0.23 | +0.03 |
| 2 | Стан (ремонт) | 0.09 | 0.10 | +0.01 |
| 3 | Час експозиції | 0.08 | 0.09 | +0.01 |
| 4 | Меблі та техніка | 0.07 | 0.08 | +0.01 |
| 5 | Планування / кімнати | 0.07 | 0.08 | +0.01 |
| 6 | Інфраструктура | 0.07 | 0.08 | +0.01 |
| 7 | Поверх | 0.06 | 0.07 | +0.01 |
| 8 | Унікальні переваги | 0.06 | 0.07 | +0.01 |
| 9 | Комунікації | 0.06 | 0.06 | — |
| 10 | Тип будинку | 0.05 | 0.05 | — |
| 11 | Попит/пропозиція (конкуренція) | 0.05 | 0.05 | — |
| 12 | Умови купівлі | 0.04 | 0.04 | — |
| 13 | Жила площа | 0.03 | 0.03 | — |
| | **СУМА** | **0.88** | **1.03** | **TODO: треба скоригувати** |

> **TODO:** Сума виходить 1.03 замість 1.00. Потрібно вирішити де зняти 0.03:
> - Варіант А: Ціна 0.23→0.20 (залишити як було)
> - Варіант Б: Зняти по 0.01 з трьох менш важливих
> - Варіант В: Залишити як є — при nullResult ваги перерозподіляються автоматично (п.4.2 ТЗ)
>
> **Рекомендація:** Варіант В — не підганяти суму штучно. Ваги = пріоритет критерію, а нормалізація робиться автоматично: `L = Σ(Si*Wi) / Σ(Wi для non-null)`

> **Жила площа — рішення прийняте:**
> - OLX (482k) не має living_area — використовувати `totalArea` для apartment/commercial
> - Решта платформ — `livingArea`, fallback на `totalArea`

---

## 3. Зміни по кожному критерію

### 3.1. Ціна (price.criterion.ts) — W=0.22

**Поточний стан:** min-max нормалізація серед аналогів. ✅ Відповідає ТЗ.
**Формула:** `S = 10 * (xmax - x) / (xmax - xmin)` — "менше краще"
**Джерело:** `subject.price`, `analogs[].price` — entity колонки, працює для всіх платформ.
**FIX потрібен:** Тільки вага 0.20 → 0.22.

### 3.2. Стан / ремонт (condition.criterion.ts) — W=0.10

**Поточний стан:** Використовує `PrimaryDataExtractor.extractCondition()`. Шкала 0-10. ✅
**ПОМИЛКА:** domRia парситься як realtorUa (main_params.status).

**FIX — extractCondition() повний рефакторинг:**

```
switch(platform):
  'olx':        → params[key="repair"].value
  'domRia':     → characteristics_values['516'] → маппінг коду (див. таблицю нижче)
  'realtorUa':  → main_params.status
  'realEstateLvivUa': → details["Стан"]
  'mlsUkraine': → params["stan(pemont)_obyekta"]
  'vector_crm': → attributes.condition_type → маппінг коду
  fallback:     → listing.condition (entity колонка, заповнена при синхронізації)
```

**domRia characteristics_values['516'] маппінг:**
```typescript
const DOMRIA_CONDITION_MAP: Record<string, { text: string; score: number }> = {
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
```

**realtorUa main_params.status маппінг:**
```
Дизайнерський ремонт → 10
Євроремонт → 9
Чудовий стан → 8
Хороший стан / З ремонтом → 7
Задовільний стан → 6
Частковий ремонт → 5
Потрібен капітальний ремонт → 0
```

**realEstateLvivUa details["Стан"] маппінг:**
```
люкс → 10
відмінний → 9
хороший → 7
задовільний → 5
потребує ремонту → 1
```

**ВАЖЛИВО:** Для domRia можна повертати score напряму з маппінгу (без keyword matching), бо код однозначно визначає стан.

### 3.3. Час експозиції (exposure-time.criterion.ts) — W=0.09

**Поточний стан:** Шкала 0-10 з fallback. ✅
**Джерело:** Розрахунок з БД (deleted_at - published_at). Не залежить від платформи.
**FIX:** Тільки вага.

### 3.4. Меблі та техніка (furniture.criterion.ts) — W=0.08

**Поточний стан:** Немає→0, Частково→5, Є→10. ✅
**ПОМИЛКА:** domRia, realtorUa, realEstateLvivUa — не парсяться.

**FIX — extractFurnish() рефакторинг:**
```
switch(platform):
  'olx':        → params[key="furnish"].value
  'domRia':     → НЕТ даних → null (nullResult)
  'realtorUa':  → НЕТ даних → null (nullResult)
  'realEstateLvivUa': → НЕТ даних → null (nullResult)
  'mlsUkraine': → params["mebli"]
  'vector_crm': → attributes.furniture (1=yes, 2=no) + rent_furniture
  fallback:     → null (nullResult)
```

### 3.5. Планування / кімнати (format.criterion.ts) — W=0.08

**Поточний стан:** rooms → фіксована шкала.
**ТЗ вимагає:** count(layout_features) з min-max нормалізацією серед аналогів.

**РІШЕННЯ:** Комбінований підхід:
1. Якщо є дані про планування → використовуємо їх як feature count
2. Якщо немає → використовуємо rooms як proxy

**Дані по планируванню по платформах:**
```
switch(platform):
  'olx':        → params[key="layout"].value (Роздільна, Суміжна, Студія...)
  'realtorUa':  → main_params.planirovka (Роздільне, Суміжно-роздільна, Студія, Кухня-вітальня, Багаторівнева, Пентхаус)
  'mlsUkraine': → params["osoblyvosti_planuvannja"]
  'vector_crm': → attributes.location_rooms
  інші:         → НЕТ → rooms fallback
```

**Шкала для rooms (fallback):**
```
1 кімната → 10 (найліквідніше)
2 кімнати → 8
3 кімнати → 6
4 кімнати → 4
5+ кімнат → 2
```

**Шкала для layout type:**
```
Студія → 10
Кухня-вітальня → 9
Роздільне → 8
Суміжно-роздільне → 6
Суміжне → 4
Пентхаус → 10
Багаторівнева → 8
```

### 3.6. Інфраструктура (infrastructure.criterion.ts) — W=0.08

**Поточний стан:** Overpass API дистанції. ✅ Працює для всіх платформ.
**FIX:** Тільки вага.

### 3.7. Поверх (floor.criterion.ts) — W=0.07

**Поточний стан:** 1-й/останній→0, 2-й→5, 3-передостанній→10. ✅ Відповідає ТЗ.
**Джерело:** `subject.floor`, `subject.totalFloors` — entity колонки.
**ПОМИЛКА:** Не враховує realtyType — для дома/коммерції 1-й поверх може бути нормою.

**FIX:**
```typescript
// Для house/commercial: 1-й поверх = 10 (норма)
if (subject.realtyType === 'house' || subject.realtyType === 'commercial') {
  return this.createResult(10, 'Тип об\'єкту: поверх не впливає');
}
// Для apartment: поточна логіка
```

### 3.8. Унікальні переваги (unique-features.criterion.ts) — W=0.06

**Поточний стан:** count(features) з min-max серед аналогів. ✅
**ПОМИЛКА:** extractComfort() — domRia не парситься, OLX comfort-строки не нормалізуються.

**FIX — extractComfort() рефакторинг:**
```
switch(platform):
  'olx':        → params[key="comfort"].value → split + normalize (укр→eng теги)
  'domRia':     → НЕТ даних → []
  'realtorUa':  → НЕТ даних → []
  'realEstateLvivUa': → НЕТ даних → []
  'mlsUkraine': → params (lift, balkon_lodzhija, harazh, kondytsionuvannja_, kamin)
  'vector_crm': → attributes (balcony_type, elevator_count, parking)
```

**Нормалізація OLX comfort:**
```typescript
const COMFORT_NORMALIZE: Record<string, string> = {
  'балкон': 'balcony',
  'лоджія': 'loggia',
  'ліфт': 'elevator',
  'кондиціонер': 'conditioner',
  'гараж': 'garage',
  'паркінг': 'parking',
  'панорамні вікна': 'panoramic_windows',
  'камін': 'fireplace',
  'тераса': 'terrace',
  'басейн': 'pool',
  'сауна': 'sauna',
};
```

### 3.9. Комунікації (communications.criterion.ts) — W=0.06

**Поточний стан:** Різні шкали apartment vs house. ✅
**ПОМИЛКИ:**
1. domRia, realtorUa, realEstateLvivUa — не парсяться
2. Для квартир при відсутності даних — нульовий бал (неправильно)

**FIX — дефолт для квартир:**
В Україні ВСІ квартири мають електрику, опалення, холодну воду.
```typescript
// Apartment default (якщо немає даних про комунікації):
if (subject.realtyType === 'apartment') {
  baseScore = 3; // електрика(1) + опалення(1) + холодна вода(1)
  // Додатково витягуємо: газ(+3), гаряча вода(+2), інтернет(+2)
}
```

**FIX — extractCommunications() рефакторинг:**
```
switch(platform):
  'olx':        → params[key="communications"].value + params[key="heating"] + params[key="bathroom"]
  'domRia':     → characteristics_values (потрібно знайти коди для gas, water, heating)
  'realtorUa':  → НЕТ структурованих даних → apartment default
  'realEstateLvivUa': → НЕТ → apartment default
  'mlsUkraine': → params (elektryka, voda, haz, opalennja, kanalizatsija, internet_tv)
  'vector_crm': → attributes (gas_type, water_type, electricity_type, sewerage_type, heating_type)
```

### 3.10. Тип будинку (house-type.criterion.ts) — W=0.06

**Поточний стан:** keyword matching по тексту. ✅
**ПОМИЛКА:** domRia парситься як realtorUa (main_params.border замість wall_type).

**FIX — extractHouseType() рефакторинг:**
```
switch(platform):
  'olx':        → params[key="house_type"].value
  'domRia':     → wall_type_uk || wall_type → маппінг (див. нижче)
  'realtorUa':  → main_params.border
  'realEstateLvivUa': → details["Матеріал стін"]
  'mlsUkraine': → params["typ_stin"]
  'vector_crm': → listing.houseType (entity, заповнена при синхронізації)
  fallback:     → listing.houseType
```

**domRia wall_type → бал маппінг:**
```typescript
const WALL_TYPE_SCORE: Record<string, number> = {
  // Моноліт (10)
  'моноліт': 10, 'монолит': 10,
  'монолітно-каркасний': 10, 'монолитно-каркасный': 10,
  'монолітно-цегляний': 10, 'монолитно-кирпичный': 10,
  'монолітно-блоковий': 10, 'монолитно-блочный': 10,
  // Цегла (8)
  'цегла': 8, 'кирпич': 8,
  'силікатна цегла': 8, 'силикатный кирпич': 8,
  // Ракушняк (7) — між цеглою і блоком
  'ракушняк': 7, 'ракушечник (ракушняк)': 7,
  // Каркас (7)
  'каркасний': 7, 'каркасный': 7,
  // Блок (6)
  'газоблок': 6, 'газобетон': 6,
  'піноблок': 6, 'пеноблок': 6,
  'керамічний блок': 6, 'керамический блок': 6,
  'керамзітобетон': 6, 'керамзитобетон': 6,
  'шлакоблок': 6,
  // Панель (4)
  'панель': 4,
  'залізобетон': 4, 'железобетон': 4,
  // Інше → 5 (не класифіковано)
};
```

### 3.11. Конкуренція / попит-пропозиція (competition.criterion.ts) — W=0.06

**Поточний стан:** Лінійна інтерполяція count аналогів ≤5→10, ≥50→0. ✅
**Джерело:** `fairPrice.analogsCount` — не залежить від платформи.
**FIX:** Тільки вага.

### 3.12. Умови купівлі (buy-conditions.criterion.ts) — W=0.04

**Поточний стан:** Адитивний base=0, єОселя+5, іпотека+5, розстрочка+5, торг+2, etc.
**ПОМИЛКИ:**
1. При відсутності даних повертає score=0 замість nullResult
2. OLX не парситься структурно
3. Текстовий пошук "торг"/"обмін" дає хибні спрацювання
4. vector_crm — не всі атрибути використовуються

**FIX — extractBuyConditions() рефакторинг:**

```
switch(platform):
  'olx':        → params[key="eoselia"] + params[key="cooperate"] + description text search
  'domRia':     → Проверить эту информацию characteristic[274]	Можлива розстрочка/кредит	Number, characteristic[1437]	Тип пропозиції	Number, characteristic[273]	Можливий торг	Number,characteristic[265]	Можливий обмін	Number
  'realtorUa':  → addition_params масив: "БЕЗ КОМІСІЇ"(+4), "ПЕРЕУСТУПКА"(+3), "єОселя"(+5), "єВідновлення"(+5)
  'realEstateLvivUa': → НЕТ → nullResult
  'mlsUkraine': → params["e_oselya"], params["umova_prodazhu"]
  'vector_crm': → (див. нижче)
```

**vector_crm повний маппінг:**
```typescript
// attributes:
credit_eoselya: 1=так → єОселя (+5)
credit_eoselya_2: 1=так → єОселя 2.0 (+5)
credit_evidnovlenya: 1=так → єВідновлення (+5)
credit_dmj: 1=так → ДМЖ (+5)
in_installments: 1=так → розстрочка (+5)
bargain: 1=так → торг (+2)
method_selling: 2 → переуступка (+3)
special_condition_sale: 1=без комісії(+4)?, 2=переуступка(+3)?, потребує уточнення кодів
commision_ssum: 0 або null → без комісії (+4)
```

**FIX — nullResult при відсутності даних:**
```typescript
if (!conditions || conditions.length === 0) {
  return this.createNullResult('Умови купівлі невідомі');
}
```

**FIX — текстовий пошук:**
```typescript
// Замість: description.includes('торг')
// Використовувати: regex word boundary
/\bторг\b/i  // "торг" але не "торговий"
/\bобмін\b/i // "обмін" але не "обмінний"
```

### 3.13. Жила площа (living-area.criterion.ts) — W=0.02

**Поточний стан:** Використовує `subject.totalArea` з min-max серед аналогів. Назва не відповідає — каже "living" але бере total.
**ТЗ:** `x = living_area_m2`, "більше краще": `S = 10 * (x - xmin)/(xmax - xmin)`

**РІШЕННЯ (затверджене):**
- **OLX** (apartment, commercial): використовувати `totalArea` (living_area відсутня на платформі)
- **OLX** (house, area): використовувати `totalArea` або `landArea`
- **Решта платформ**: `livingArea`, fallback на `totalArea` якщо livingArea = null

**Доступність living_area по платформах:**
| Платформа | living_area | total_area |
|-----------|-----------|------------|
| OLX (482k) | 0% | 95% |
| vector_crm (394k) | 69% | 85% |
| domRia (274k) | 70% | 97% |
| realtorUa (263k) | 87% | 94% |
| realEstateLvivUa (72k) | 45% | 98% |
| mlsUkraine (5k) | 0% | 92% |

**FIX:**
```typescript
private getAreaValue(subject: UnifiedListingEntity): number | null {
  const platform = subject.realtyPlatform;

  // OLX та MLS не мають living_area — одразу totalArea
  if (platform === 'olx' || platform === 'mlsUkraine') {
    return subject.totalArea || null;
  }

  // Решта — livingArea з fallback на totalArea
  return subject.livingArea || subject.totalArea || null;
}
```

---

## 4. Зміни в PrimaryDataExtractor — загальна структура

### 4.1. Нова структура методів

```typescript
@Injectable()
export class PrimaryDataExtractor {

  // === ПРИВАТНИЙ МЕТОД ВИЗНАЧЕННЯ ПЛАТФОРМИ ===
  private getPlatform(listing: UnifiedListingEntity): string {
    return listing.realtyPlatform || 'unknown';
  }

  // === ДОПОМІЖНІ МЕТОДИ ПО ПЛАТФОРМАХ ===
  private getOlxParam(pd: any, key: string): string | null { ... }
  private getDomRiaCharValue(pd: any, charId: string): string | null { ... }
  private getRealtorUaMainParam(pd: any, key: string): string | null { ... }
  private getRealEstateLvivDetail(pd: any, key: string): string | null { ... }
  private getMlsParam(pd: any, key: string): string | null { ... }

  // === EXTRACT МЕТОДИ (публічні) ===
  // Кожен робить switch(this.getPlatform(listing))
  extractCondition(listing): string | null { ... }
  extractHouseType(listing): string | null { ... }
  extractFurnish(listing): 'yes' | 'no' | 'partial' | null { ... }
  extractCommunications(listing): string[] { ... }
  extractComfort(listing): string[] { ... }
  extractBuyConditions(listing): BuyCondition[] { ... }
  extractLayout(listing): string | null { ... }
  getDescriptionText(listing): string { ... }
}
```

### 4.2. Видалити зайве

- Видалити `extractDomRiaMainParam()` — це був realtorUa
- Видалити `extractDomRiaAdditionParams()` — перейменувати в `getRealtorUaAdditionParams()`
- Видалити `extractMlsParam()` → замінити на `getMlsParam()`
- Видалити `getMlsParams()` → замінити на `getMlsParam()`
- Видалити `mapConditionType()` → замінити на маппінг-константи

---

## 5. Файли для зміни

### Критерії:
1. `apps/valuation/src/modules/liquidity/criteria/base.criterion.ts` — ваги
2. `apps/valuation/src/modules/liquidity/criteria/price.criterion.ts` — вага
3. `apps/valuation/src/modules/liquidity/criteria/condition.criterion.ts` — entity-first + keyword update
4. `apps/valuation/src/modules/liquidity/criteria/floor.criterion.ts` — realtyType check
5. `apps/valuation/src/modules/liquidity/criteria/furniture.criterion.ts` — вага
6. `apps/valuation/src/modules/liquidity/criteria/format.criterion.ts` — layout type support
7. `apps/valuation/src/modules/liquidity/criteria/communications.criterion.ts` — apartment default
8. `apps/valuation/src/modules/liquidity/criteria/buy-conditions.criterion.ts` — nullResult + regex
9. `apps/valuation/src/modules/liquidity/criteria/unique-features.criterion.ts` — вага
10. `apps/valuation/src/modules/liquidity/criteria/exposure-time.criterion.ts` — вага
11. `apps/valuation/src/modules/liquidity/criteria/competition.criterion.ts` — вага
12. `apps/valuation/src/modules/liquidity/criteria/house-type.criterion.ts` — wall_type map
13. `apps/valuation/src/modules/liquidity/criteria/infrastructure.criterion.ts` — вага

### Видалити:
14. `apps/valuation/src/modules/liquidity/criteria/location.criterion.ts` — видалити (вид з вікон + природа + локація → зараз це все в location, критерій більше не потрібен)

### Сервіси:
15. `apps/valuation/src/modules/liquidity/services/primary-data-extractor.ts` — ПОВНИЙ РЕФАКТОРИНГ

### Модуль:
16. `apps/valuation/src/modules/liquidity/liquidity.module.ts` — видалити LocationCriterion з providers
17. `apps/valuation/src/modules/liquidity/liquidity.service.ts` — перевірити що location не використовується

### UI:
18. `public/app.js` — оновити тултіпи, видалити location/природа/вид з вікон

---

## 6. Порядок виконання

### Етап 1: PrimaryDataExtractor (найважливіше)
1. Додати `getPlatform()` метод
2. Рефакторинг `extractCondition()` — switch по платформах
3. Рефакторинг `extractHouseType()` — switch по платформах
4. Рефакторинг `extractFurnish()` — switch по платформах
5. Рефакторинг `extractCommunications()` — switch + apartment default
6. Рефакторинг `extractComfort()` — switch + нормалізація OLX
7. Рефакторинг `extractBuyConditions()` — switch + vector_crm атрибути + regex
8. Рефакторинг `extractLayout()` — switch по платформах
9. Рефакторинг `getDescriptionText()` — switch по платформах
10. Видалити старі методи (extractDomRiaMainParam, etc.)

### Етап 2: Критерії
11. base.criterion.ts — нові ваги
12. condition.criterion.ts — entity-first, domRia score map
13. house-type.criterion.ts — wall_type score map
14. floor.criterion.ts — realtyType check
15. communications.criterion.ts — apartment default base=3
16. buy-conditions.criterion.ts — nullResult + regex
17. format.criterion.ts — layout type support
18. Видалити location.criterion.ts

### Етап 3: Перевірка
19. `yarn build` — компіляція
20. Тест на об'єкті OLX
21. Тест на об'єкті domRia
22. Тест на об'єкті realtorUa
23. Тест на об'єкті vector_crm
24. Порівняти результати до/після

---

## 7. Маппінг-константи (новий файл)

Створити файл: `apps/valuation/src/modules/liquidity/constants/platform-mappings.ts`

Вміст:
- `DOMRIA_CONDITION_516` — characteristics_values['516'] → {text, score}
- `DOMRIA_WALL_TYPE_SCORE` — wall_type → score
- `REALTOR_CONDITION_SCORE` — main_params.status → score
- `REAL_ESTATE_CONDITION_SCORE` — details.Стан → score
- `VECTOR_CONDITION_TYPE_SCORE` — attributes.condition_type → {text, score}
- `OLX_COMFORT_NORMALIZE` — укр.назва → eng tag
- `LAYOUT_TYPE_SCORE` — тип планування → score

---

## 8. Контрольні перевірки

1. Сума ваг = 1.00 (±0.001)
2. Всі score в діапазоні [0..10]
3. xmin == xmax → score = 10
4. Відсутність даних → nullResult (не score=0)
5. Apartment default для комунікацій = base 3
6. domRia condition парситься з characteristics_values['516'], НЕ з main_params
7. realtorUa condition парситься з main_params.status
8. Платформа визначається по listing.realtyPlatform, НЕ по структурі JSON
9. Видалено: location, вид з вікон, природа, тип вікон
10. `yarn build` проходить без помилок

---

## 9. Відкриті питання

1. **special_condition_sale коди (vector_crm):** Значення 1-5 потребують розшифровки з vector-api. Потрібно перевірити `D:\analogis\vector-api\db\migrations\data\attributes-values.json`
2. **Жила площа:** Видаляємо критерій чи залишаємо? (livingArea є тільки у 49% записів)
3. **domRia characteristics_values для комунікацій:** Потрібно знайти коди для gas, water, heating в characteristics_values
4. **Перерозподіл ваг:** Поточний розподіл +0.01-0.02 — чи влаштовує?
