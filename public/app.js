// API Base URL
const API_BASE = '/api/v1/valuation';

// Current report data for dynamic tooltips
let currentReportData = null;

// DOM Elements
const elements = {
  // Tabs
  tabBtns: document.querySelectorAll('.tab-btn'),
  tabContents: document.querySelectorAll('.tab-content'),

  // Inputs
  objectId: document.getElementById('objectId'),
  sourceType: document.getElementById('sourceType'),
  externalUrl: document.getElementById('externalUrl'),
  searchBtn: document.getElementById('searchBtn'),

  // Sections
  errorSection: document.getElementById('error'),
  errorMessage: document.querySelector('.error-message'),
  resultsSection: document.getElementById('results'),

  // Property
  propertyAddress: document.getElementById('propertyAddress'),
  propertyExternalLink: document.getElementById('propertyExternalLink'),
  propertyComplex: document.getElementById('propertyComplex'),
  propertyArea: document.getElementById('propertyArea'),
  propertyRooms: document.getElementById('propertyRooms'),
  propertyFloor: document.getElementById('propertyFloor'),
  askingPrice: document.getElementById('askingPrice'),

  // Liquidity
  scoreCircle: document.getElementById('scoreCircle'),
  liquidityScore: document.getElementById('liquidityScore'),
  liquidityLevel: document.getElementById('liquidityLevel'),
  estimatedDays: document.getElementById('estimatedDays'),
  criteriaGrid: document.getElementById('criteriaGrid'),
  recommendations: document.getElementById('recommendations'),

  // Fair Price
  medianPrice: document.getElementById('medianPrice'),
  averagePrice: document.getElementById('averagePrice'),
  priceRange: document.getElementById('priceRange'),
  meterRange: document.getElementById('meterRange'),
  meterMarker: document.getElementById('meterMarker'),
  meterMin: document.getElementById('meterMin'),
  meterMax: document.getElementById('meterMax'),
  verdictBox: document.getElementById('verdictBox'),
  verdictIcon: document.getElementById('verdictIcon'),
  verdictText: document.getElementById('verdictText'),
  ppmObject: document.getElementById('ppmObject'),
  ppmMarket: document.getElementById('ppmMarket'),
  ppmDiff: document.getElementById('ppmDiff'),
  priceHistogram: document.getElementById('priceHistogram'),
  histogramLabels: document.getElementById('histogramLabels'),

  // Analogs
  analogsCount: document.getElementById('analogsCount'),
  searchRadius: document.getElementById('searchRadius'),
  analogsTableBody: document.getElementById('analogsTableBody'),

  // Report
  reportId: document.getElementById('reportId'),
  generatedAt: document.getElementById('generatedAt'),
  confidence: document.getElementById('confidence'),
};

// Translations
const translations = {
  levels: {
    high: 'Висока ліквідність',
    medium: 'Середня ліквідність',
    low: 'Низька ліквідність',
  },
  verdicts: {
    cheap: { text: 'Нижче ринку', icon: '↓' },
    in_market: { text: 'В ринку', icon: '≈' },
    expensive: { text: 'Вище ринку', icon: '↑' },
  },
  criteria: {
    price: 'Ціна',
    livingArea: 'Житлова площа',
    exposureTime: 'Час експозиції',
    competition: 'Конкуренція',
    infrastructure: 'Інфраструктура',
    condition: 'Стан',
    format: 'Формат',
    floor: 'Поверх',
    houseType: 'Тип будинку',
    furniture: 'Меблі та техніка',
    communications: 'Комунікації',
    uniqueFeatures: 'Унікальні переваги',
    buyConditions: 'Умови купівлі',
  },
  criteriaTooltips: {
    price: {
      formula: 'Min-max нормалізація серед аналогів:\nS = 10 × (xmax - x) / (xmax - xmin)\n\n"Менше краще" — дешевший = вищий бал\nДіапазон: 0–10\n\nFallback (без аналогів):\n  ratio ≤ 0.85 → 10\n  ratio ≤ 0.95 → 8\n  ratio ≤ 1.05 → 6\n  ratio ≤ 1.15 → 4\n  ratio ≤ 1.30 → 2\n  ratio > 1.30 → 0',
      example: 'Об\'єкт: $50,000, аналоги: $45,000–$80,000\nS = 10 × (80000 - 50000) / (80000 - 45000)\nS = 10 × 0.857 = 8.6',
    },
    livingArea: {
      formula: 'S = 10 × (x - xmin) / (xmax - xmin)\n\n"Більше краще" — більша площа = вищий бал\nЯкщо xmin == xmax → S = 10\nДіапазон: 0–10\n\nOLX/MLS: totalArea (living_area відсутня)\nРешта: livingArea, fallback totalArea',
      example: 'Об\'єкт: 65 м², аналоги: 40–90 м²\nS = 10 × (65-40)/(90-40) = 5.0',
    },
    exposureTime: {
      formula: 'Базовий час (дні): квартира=30, дім=58,\nкомерція=51, ділянка=68\n\nМножник ціни:\n  дешевше → ×0.6, в ринку → ×1.0, дорожче → ×1.5\n\nratio = оцінка / медіана ринку\n  ≤0.5→10, ≤0.8→8, ≤1.0→6, ≤1.2→4, ≤1.5→2, >1.5→0\n\nFallback: дешево→10, в ринку→5, дорого→0',
      example: 'Квартира, ціна в ринку:\nоцінка = 30 × 1.0 = 30 днів\nмедіана = 35 днів, ratio = 0.86 → бал 8',
    },
    competition: {
      formula: 'Лінійна інтерполяція:\n  ≤ 5 аналогів → 10\n  ≥ 50 аналогів → 0\n  Між 5–50: S = 10 × (50 - count) / 45\n\nМенше конкурентів = легше продати\nДіапазон: 0–10',
      example: '20 аналогів:\nS = 10 × (50-20)/45 = 6.7',
    },
    infrastructure: {
      formula: 'Зважена оцінка відстаней до:\n  Транспорт — вага 30%\n  Школа — вага 25%\n  Супермаркет — вага 20%\n  Лікарня — вага 15%\n  Парковка — вага 10%\n\nОцінка відстані:\n  0–300м → 9–10\n  300–500м → 7–9\n  500–800м → 5–7\n  800–1200м → 3–5\n  > 1200м → 1–3',
      example: 'Транспорт 200м (9.3), школа 400м (8.2),\nсупермаркет 100м (9.7), лікарня 900м (4.5)\nбал = (9.3×0.30 + 8.2×0.25 + 9.7×0.20\n       + 4.5×0.15) / 0.90 = 8.2',
    },
    condition: {
      formula: 'Стан об\'єкта → бал (0–10):\n  аварійний → 0\n  потребує ремонту → 1\n  без ремонту → 1\n  чорнова штукатурка → 2\n  після будівельників → 3\n  косметичний → 5\n  задовільний → 6\n  хороший/житловий → 7\n  євроремонт → 9\n  дизайнерський → 10\n\nDomRia: characteristics_values[516]\nRealtorUa: main_params.status\nVector: condition_type',
      example: 'Стан: "Євроремонт" → бал 9',
    },
    format: {
      formula: 'Планування (якщо є дані):\n  студія → 10, кухня-вітальня → 9\n  роздільне → 8, суміжно-роздільне → 6\n  суміжне → 4, пентхаус → 10\n\nFallback кімнати:\n  1-кімн → 10, 2-кімн → 8\n  3-кімн → 6, 4-кімн → 4, 5+ → 2',
      example: '2-кімнатна, планування "Роздільне" → бал 8',
    },
    floor: {
      formula: 'Квартира (0–10):\n  1-й поверх → 0\n  останній поверх → 0\n  2-й поверх → 5\n  високий (> 80%) → 8\n  3-й до передостаннього → 10\n\nДім/ділянка: не застосовується\nКомерція: 1-й поверх → 10 (перевага)',
      example: 'Квартира, поверх 4 з 9 → бал 10\nКомерція, 1 поверх → бал 10',
    },
    houseType: {
      formula: 'Тип будинку → бал (0–10):\n  хрущовка → 1\n  старий фонд → 3\n  панельний → 4\n  блочний/газоблок → 6\n  каркасний → 7\n  цегла → 8\n  моноліт → 10\n  не розпізнано → 5',
      example: 'Тип: "Цегляний" → бал 8',
    },
    furniture: {
      formula: 'Наявність меблів (0–10):\n  Немає → 0\n  Частково → 5\n  Є → 10\n\nДжерело: OLX "furnish"\nабо Vector2 attributes.furniture',
      example: 'Меблі: "Так" → бал 10',
    },
    communications: {
      formula: 'Квартира (base=3: електрика+опалення+вода):\n  інтернет+2, гаряча вода+2, газ+3\n  макс 10\n\nДім/комерція:\n  водопровід+1.5, септик+1, інтернет+0.5,\n  каналізація+1.5, колодязь+1, свердловина+1,\n  газ+1.5, електрика+1.5\n\nS = min(10, сума)\nПри відсутності даних: квартира→3, інше→null',
      example: 'Квартира: base(3) + газ(3) + інтернет(2) = 8',
    },
    uniqueFeatures: {
      formula: 'x = кількість переваг (comfort-теги)\nMin-max серед аналогів:\nS = 10 × (x - xmin) / (xmax - xmin)\n\nЯкщо xmin == xmax → S = 10\nДіапазон: 0–10',
      example: 'Об\'єкт: 4 переваги, аналоги: 1–6\nS = 10 × (4-1)/(6-1) = 6.0',
    },
    buyConditions: {
      formula: 'Базовий бал = 0, додаються бонуси:\n  єОселя → +5\n  єВідновлення → +5\n  ДМЖ → +5\n  іпотека/кредит → +5\n  розстрочка → +5\n  переуступка → +3\n  без комісії → +4\n  торг → +2\n  обмін → +1\n\nS = min(10, сума)',
      example: 'єОселя(+5) + торг(+2)\nбал = min(10, 0 + 7) = 7',
    },
  },
  searchRadius: {
    building: 'Той самий будинок',
    block: 'Той самий квартал (200м)',
    street: 'Та сама вулиця',
    topzone: 'Той самий мікрорайон',
    district: 'Той самий район',
    neighbor_districts: 'Сусідні райони',
    city: 'Все місто',
  },
};

// Initialize
function init() {
  // Tab switching
  elements.tabBtns.forEach(btn => {
    btn.addEventListener('click', () => switchTab(btn.dataset.tab));
  });

  // Search button
  elements.searchBtn.addEventListener('click', handleSearch);

  // Enter key
  elements.objectId.addEventListener('keypress', e => {
    if (e.key === 'Enter') handleSearch();
  });
  elements.externalUrl.addEventListener('keypress', e => {
    if (e.key === 'Enter') handleSearch();
  });

  // Check URL params
  const params = new URLSearchParams(window.location.search);
  const id = params.get('id');
  const source = params.get('source');
  const url = params.get('url');

  if (source) {
    elements.sourceType.value = source;
  }

  if (id) {
    elements.objectId.value = id;
    document.querySelector('.search-section').classList.add('hidden');
    handleSearch();
  } else if (url) {
    switchTab('url');
    elements.externalUrl.value = url;
    document.querySelector('.search-section').classList.add('hidden');
    handleSearch();
  }
}

function switchTab(tab) {
  elements.tabBtns.forEach(btn => {
    btn.classList.toggle('active', btn.dataset.tab === tab);
  });
  elements.tabContents.forEach(content => {
    content.classList.toggle('active', content.id === `tab-${tab}`);
  });
}

async function handleSearch() {
  const activeTab = document.querySelector('.tab-btn.active').dataset.tab;
  let searchId = '';
  let sourceType = '';

  if (activeTab === 'id') {
    searchId = elements.objectId.value.trim();
    sourceType = elements.sourceType.value;
  } else {
    const url = elements.externalUrl.value.trim();
    if (url) {
      // First find the listing by external URL
      try {
        const listing = await findByExternalUrl(url);
        if (listing) {
          searchId = listing.id;
        } else {
          showError('Об\'єкт з таким URL не знайдено');
          return;
        }
      } catch (error) {
        showError('Помилка пошуку по URL: ' + error.message);
        return;
      }
    }
  }

  if (!searchId) {
    showError('Введіть ID об\'єкта або URL');
    return;
  }

  setLoading(true);
  hideError();
  hideResults();

  try {
    let url = `${API_BASE}/${searchId}/full?refresh=true`;
    if (sourceType) {
      url += `&source=${sourceType}`;
    }

    const response = await fetch(url);
    if (!response.ok) {
      if (response.status === 404) {
        throw new Error('Об\'єкт не знайдено');
      }
      throw new Error(`Помилка сервера: ${response.status}`);
    }

    const data = await response.json();
    displayResults(data);
  } catch (error) {
    showError(error.message);
  } finally {
    setLoading(false);
  }
}

async function findByExternalUrl(url) {
  const response = await fetch(`/api/v1/listings/search?external_url=${encodeURIComponent(url)}`);
  if (!response.ok) {
    if (response.status === 404) return null;
    throw new Error('Помилка пошуку');
  }
  return response.json();
}

function setLoading(loading) {
  elements.searchBtn.disabled = loading;
  elements.searchBtn.querySelector('.btn-text').classList.toggle('hidden', loading);
  elements.searchBtn.querySelector('.btn-loader').classList.toggle('hidden', !loading);
}

function showError(message) {
  elements.errorMessage.textContent = message;
  elements.errorSection.classList.remove('hidden');
}

function hideError() {
  elements.errorSection.classList.add('hidden');
}

function hideResults() {
  elements.resultsSection.classList.add('hidden');
}

function displayResults(data) {
  // Store for dynamic tooltips
  currentReportData = data;

  // Property Info
  displayPropertyInfo(data.property);

  // Liquidity
  displayLiquidity(data.liquidity);

  // Fair Price
  displayFairPrice(data.fairPrice, data.property?.askingPrice, data.analogs);

  // Analogs
  displayAnalogs(data.analogs);

  // Report Info
  displayReportInfo(data);

  elements.resultsSection.classList.remove('hidden');
}

function displayPropertyInfo(property) {
  if (!property) return;

  elements.propertyAddress.textContent = property.address || 'Адреса не вказана';

  // External link
  if (property.externalUrl) {
    elements.propertyExternalLink.href = property.externalUrl;
    elements.propertyExternalLink.classList.remove('hidden');
  } else {
    elements.propertyExternalLink.classList.add('hidden');
  }

  // Show complex name if available
  if (property.complexName) {
    elements.propertyComplex.classList.remove('hidden');
    elements.propertyComplex.querySelector('.complex-name').textContent = property.complexName;
  } else {
    elements.propertyComplex.classList.add('hidden');
  }

  elements.propertyArea.innerHTML = `<strong>Площа:</strong> ${property.area ? property.area + ' м²' : '-'}`;
  elements.propertyRooms.innerHTML = `<strong>Кімнат:</strong> ${property.rooms || '-'}`;
  elements.propertyFloor.innerHTML = `<strong>Поверх:</strong> ${property.floor ? property.floor + (property.totalFloors ? '/' + property.totalFloors : '') : '-'}`;
  elements.askingPrice.textContent = property.askingPrice ? formatPrice(property.askingPrice) : '-';
}

function displayLiquidity(liquidity) {
  if (!liquidity) return;

  // Score circle
  const score = liquidity.score || 0;
  const circumference = 283; // 2 * PI * 45
  const offset = circumference - (score / 10) * circumference;

  elements.scoreCircle.style.strokeDashoffset = offset;
  elements.scoreCircle.classList.remove('high', 'medium', 'low');
  elements.scoreCircle.classList.add(liquidity.level || 'medium');

  elements.liquidityScore.textContent = score.toFixed(1);

  // Level
  elements.liquidityLevel.textContent = translations.levels[liquidity.level] || liquidity.level;
  elements.liquidityLevel.classList.remove('high', 'medium', 'low');
  elements.liquidityLevel.classList.add(liquidity.level || 'medium');

  // Confidence badge
  const confidenceBadge = document.getElementById('confidenceBadge');
  if (confidenceBadge) {
    if (liquidity.confidence) {
      const confLabels = { high: 'Висока достовірність', medium: 'Середня достовірність', low: 'Низька достовірність' };
      confidenceBadge.textContent = confLabels[liquidity.confidence] || '';
      confidenceBadge.className = `confidence-badge ${liquidity.confidence}`;
      confidenceBadge.classList.remove('hidden');
    } else {
      confidenceBadge.classList.add('hidden');
    }
  }

  // Days
  elements.estimatedDays.textContent = `${liquidity.estimatedDaysToSell || '-'} днів`;

  // Criteria
  displayCriteria(liquidity.criteria);

  // Recommendations
  displayRecommendations(liquidity.recommendations);
}

function displayCriteria(criteria) {
  if (!criteria || !criteria.length) {
    elements.criteriaGrid.innerHTML = '<p class="no-data">Немає даних</p>';
    return;
  }

  const evaluated = criteria.filter(c => c.weight > 0);
  const missing = criteria.filter(c => c.weight === 0);

  let html = evaluated.map(c => {
    const level = c.score >= 7 ? 'high' : c.score >= 5 ? 'medium' : 'low';
    const name = translations.criteria[c.name] || c.name;
    const tooltip = translations.criteriaTooltips[c.name];
    const tooltipIcon = tooltip
      ? `<span class="criterion-tooltip-icon" data-criterion="${c.name}" title="Як розраховується">?</span>`
      : '';

    return `
      <div class="criterion-item">
        <div class="criterion-header">
          <span class="criterion-name">${name}${tooltipIcon}</span>
          <span class="criterion-score ${level}">${c.score.toFixed(1)}</span>
        </div>
        <div class="criterion-bar">
          <div class="criterion-bar-fill ${level}" style="width: ${c.score * 10}%"></div>
        </div>
        <div class="criterion-explanation">${c.explanation || ''}</div>
      </div>
    `;
  }).join('');

  if (missing.length > 0) {
    html += missing.map(c => {
      const name = translations.criteria[c.name] || c.name;
      const tooltip = translations.criteriaTooltips[c.name];
      const tooltipIcon = tooltip
        ? `<span class="criterion-tooltip-icon" data-criterion="${c.name}" title="Як розраховується">?</span>`
        : '';
      return `
        <div class="criterion-item criterion-missing">
          <div class="criterion-header">
            <span class="criterion-name">${name}${tooltipIcon}</span>
            <span class="criterion-score missing">—</span>
          </div>
          <div class="criterion-explanation missing-text">${c.explanation || 'Немає даних'}</div>
        </div>
      `;
    }).join('');
  }

  elements.criteriaGrid.innerHTML = html;

  // Attach tooltip modal handlers
  elements.criteriaGrid.querySelectorAll('.criterion-tooltip-icon').forEach(icon => {
    const criterionKey = icon.dataset.criterion;
    const tooltip = translations.criteriaTooltips[criterionKey];
    if (!tooltip) return;

    icon.addEventListener('click', (e) => {
      e.stopPropagation();
      const criterionName = translations.criteria[criterionKey] || criterionKey;
      const dynamicExample = buildDynamicExample(criterionKey);
      const exampleText = dynamicExample || tooltip.example;

      showCriterionModal(criterionName, tooltip.formula, exampleText, !!dynamicExample);
    });
  });
}


function showCriterionModal(title, formula, example, isDynamic) {
  // Remove existing modal if any
  closeCriterionModal();

  const overlay = document.createElement('div');
  overlay.className = 'criterion-modal-overlay';
  overlay.innerHTML = `
    <div class="criterion-modal">
      <div class="criterion-modal-header">
        <span class="criterion-modal-title">${escapeHtml(title)}</span>
        <button class="criterion-modal-close" aria-label="Закрити">&times;</button>
      </div>
      <div class="criterion-modal-body">
        <div class="tooltip-section">
          <div class="tooltip-section-title">Формула</div>
          <pre class="tooltip-formula">${escapeHtml(formula)}</pre>
        </div>
        <div class="tooltip-section">
          <div class="tooltip-section-title">${isDynamic ? 'Ваш об\'єкт' : 'Приклад'}</div>
          <pre class="tooltip-example">${escapeHtml(example)}</pre>
        </div>
      </div>
    </div>
  `;

  overlay.addEventListener('click', (e) => {
    if (e.target === overlay) closeCriterionModal();
  });
  overlay.querySelector('.criterion-modal-close').addEventListener('click', closeCriterionModal);

  document.body.appendChild(overlay);
  requestAnimationFrame(() => overlay.classList.add('visible'));

  document.addEventListener('keydown', criterionModalEscHandler);
}

function criterionModalEscHandler(e) {
  if (e.key === 'Escape') closeCriterionModal();
}

function closeCriterionModal() {
  document.removeEventListener('keydown', criterionModalEscHandler);
  const overlay = document.querySelector('.criterion-modal-overlay');
  if (overlay) {
    overlay.classList.remove('visible');
    setTimeout(() => overlay.remove(), 150);
  }
}

function buildDynamicExample(criterionName) {
  if (!currentReportData) return null;

  const property = currentReportData.property;
  const analogs = currentReportData.analogs?.analogs || [];
  const liquidity = currentReportData.liquidity;
  const criterion = liquidity?.criteria?.find(c => c.name === criterionName);
  const score = criterion?.score;

  if (score === undefined || score === null) return null;
  const s = score.toFixed(1);

  switch (criterionName) {
    case 'price': {
      const price = property?.askingPrice;
      const prices = analogs.map(a => a.price).filter(p => p > 0).sort((a, b) => a - b);
      if (!price || prices.length < 2) return null;
      const min = prices[0];
      const max = prices[prices.length - 1];
      const calc = max !== min ? (10 * (max - price) / (max - min)).toFixed(1) : '10.0';
      return `Ціна об'єкта: ${formatPrice(price)}\nАналоги: ${formatPrice(min)} – ${formatPrice(max)} (${prices.length} шт)\n\nS = 10 × (${formatPriceShort(max)} - ${formatPriceShort(price)}) / (${formatPriceShort(max)} - ${formatPriceShort(min)})\nS = ${calc}\n\nБал: ${s}`;
    }
    case 'livingArea': {
      const area = property?.area;
      const areas = analogs.map(a => a.area).filter(a => a > 0).sort((a, b) => a - b);
      if (!area || areas.length < 2) return null;
      const min = areas[0];
      const max = areas[areas.length - 1];
      const calc = max !== min ? (10 * (area - min) / (max - min)).toFixed(1) : '10.0';
      return `Площа об'єкта: ${area} м²\nАналоги: ${min} – ${max} м² (${areas.length} шт)\n\nS = 10 × (${area} - ${min}) / (${max} - ${min})\nS = ${calc}\n\nБал: ${s}`;
    }
    case 'competition': {
      const count = analogs.length;
      let calc;
      if (count <= 5) calc = '10.0';
      else if (count >= 50) calc = '0.0';
      else calc = (10 * (50 - count) / 45).toFixed(1);
      return `Кількість аналогів: ${count}\n\nS = 10 × (50 - ${count}) / 45 = ${calc}\n\nБал: ${s}`;
    }
    case 'floor': {
      const floor = property?.floor;
      const totalFloors = property?.totalFloors;
      if (!floor) return null;
      let desc = '';
      if (floor === 1) desc = '1-й поверх → 0 балів';
      else if (floor === totalFloors) desc = `останній поверх (${floor}/${totalFloors}) → 0 балів`;
      else if (floor === 2) desc = `2-й поверх з ${totalFloors || '?'} → 5 балів`;
      else if (totalFloors && floor / totalFloors > 0.8) desc = `високий поверх (${floor}/${totalFloors}, ${Math.round(floor/totalFloors*100)}%) → 8 балів`;
      else desc = `${floor}-й поверх з ${totalFloors || '?'} (середній) → 10 балів`;
      return `Поверх: ${floor}${totalFloors ? '/' + totalFloors : ''}\n${desc}\n\nБал: ${s}`;
    }
    case 'format': {
      const rooms = property?.rooms;
      if (!rooms) return null;
      const roomsDesc = { 1: '1-кімн → 10', 2: '2-кімн → 8', 3: '3-кімн → 6', 4: '4-кімн → 4' };
      const desc = roomsDesc[rooms] || `${rooms}-кімн → 2`;
      return `Кімнат: ${rooms}\n${desc}\n\nБал: ${s}`;
    }
    case 'exposureTime': {
      const days = liquidity?.estimatedDaysToSell;
      if (!days) return null;
      return `Орієнтовний час продажу: ${days} днів\n\nБал: ${s}`;
    }
    case 'condition': {
      const explanation = criterion?.explanation;
      return explanation
        ? `${explanation}\n\nБал: ${s}`
        : null;
    }
    case 'houseType': {
      const explanation = criterion?.explanation;
      return explanation
        ? `${explanation}\n\nБал: ${s}`
        : null;
    }
    case 'furniture': {
      const explanation = criterion?.explanation;
      return explanation
        ? `${explanation}\n\nБал: ${s}`
        : null;
    }
    case 'communications': {
      const explanation = criterion?.explanation;
      return explanation
        ? `${explanation}\n\nБал: ${s}`
        : null;
    }
    case 'infrastructure': {
      const explanation = criterion?.explanation;
      return explanation
        ? `${explanation}\n\nБал: ${s}`
        : null;
    }
    case 'uniqueFeatures': {
      const explanation = criterion?.explanation;
      return explanation
        ? `${explanation}\n\nБал: ${s}`
        : null;
    }
    case 'buyConditions': {
      const explanation = criterion?.explanation;
      return explanation
        ? `${explanation}\n\nБал: ${s}`
        : null;
    }
    default:
      return null;
  }
}


function escapeHtml(text) {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

function displayRecommendations(recommendations) {
  if (!recommendations || !recommendations.length) {
    elements.recommendations.classList.add('hidden');
    return;
  }

  elements.recommendations.classList.remove('hidden');
  elements.recommendations.innerHTML = `
    <h4>Рекомендації</h4>
    <ul>
      ${recommendations.map(r => `<li>${r}</li>`).join('')}
    </ul>
  `;
}

function displayFairPrice(fairPrice, askingPrice, analogs) {
  if (!fairPrice) return;

  // Stats
  elements.medianPrice.textContent = formatPrice(fairPrice.median);
  elements.averagePrice.textContent = formatPrice(fairPrice.average);
  elements.priceRange.textContent = `${formatPrice(fairPrice.range?.low)} - ${formatPrice(fairPrice.range?.high)}`;

  // Meter
  const min = fairPrice.min || fairPrice.range?.low || 0;
  const max = fairPrice.max || fairPrice.range?.high || 100000;
  const rangeLow = fairPrice.range?.low || min;
  const rangeHigh = fairPrice.range?.high || max;

  const rangeStart = ((rangeLow - min) / (max - min)) * 100;
  const rangeWidth = ((rangeHigh - rangeLow) / (max - min)) * 100;

  elements.meterRange.style.left = `${rangeStart}%`;
  elements.meterRange.style.width = `${rangeWidth}%`;

  if (askingPrice) {
    const markerPos = Math.min(100, Math.max(0, ((askingPrice - min) / (max - min)) * 100));
    elements.meterMarker.style.left = `${markerPos}%`;
    elements.meterMarker.style.display = 'block';
  } else {
    elements.meterMarker.style.display = 'none';
  }

  elements.meterMin.textContent = formatPrice(min);
  elements.meterMax.textContent = formatPrice(max);

  // Verdict
  const verdict = fairPrice.verdict || 'in_market';
  const verdictInfo = translations.verdicts[verdict] || { text: verdict, icon: '•' };

  elements.verdictBox.className = `verdict-box ${verdict}`;
  elements.verdictIcon.textContent = verdictInfo.icon;
  elements.verdictText.textContent = verdictInfo.text;

  // Price per meter
  const ppmMarket = fairPrice.pricePerMeter?.median || 0;
  const ppmObject = askingPrice && fairPrice.analogsCount > 0
    ? Math.round(askingPrice / (fairPrice.pricePerMeter?.median ? (fairPrice.median / fairPrice.pricePerMeter.median) : 1))
    : 0;

  elements.ppmObject.textContent = ppmObject ? `$${ppmObject}` : '-';
  elements.ppmMarket.textContent = ppmMarket ? `$${ppmMarket}` : '-';

  if (ppmObject && ppmMarket) {
    const diff = ppmObject - ppmMarket;
    const diffPercent = ((diff / ppmMarket) * 100).toFixed(1);
    elements.ppmDiff.textContent = `${diff > 0 ? '+' : ''}$${diff} (${diff > 0 ? '+' : ''}${diffPercent}%)`;
    elements.ppmDiff.classList.toggle('positive', diff < 0);
    elements.ppmDiff.classList.toggle('negative', diff > 0);
  } else {
    elements.ppmDiff.textContent = '-';
    elements.ppmDiff.classList.remove('positive', 'negative');
  }

  // Price Histogram
  renderPriceHistogram(analogs, askingPrice);
}

function renderPriceHistogram(analogsData, askingPrice) {
  const analogs = analogsData?.analogs || analogsData || [];

  if (!Array.isArray(analogs) || analogs.length === 0) {
    elements.priceHistogram.innerHTML = '<div style="text-align:center;color:var(--gray-400);padding:2rem;">Немає даних</div>';
    elements.histogramLabels.innerHTML = '';
    return;
  }

  // Get prices from analogs
  const prices = analogs.map(a => a.price).filter(p => p && p > 0).sort((a, b) => a - b);
  if (prices.length === 0) {
    elements.priceHistogram.innerHTML = '<div style="text-align:center;color:var(--gray-400);padding:2rem;">Немає даних</div>';
    elements.histogramLabels.innerHTML = '';
    return;
  }

  // Calculate histogram bins
  const minPrice = prices[0];
  const maxPrice = prices[prices.length - 1];
  const numBins = Math.min(12, Math.max(5, Math.ceil(prices.length / 2)));
  const binWidth = (maxPrice - minPrice) / numBins;

  // Create bins
  const bins = [];
  for (let i = 0; i < numBins; i++) {
    bins.push({
      min: minPrice + i * binWidth,
      max: minPrice + (i + 1) * binWidth,
      count: 0
    });
  }

  // Fill bins
  prices.forEach(price => {
    const binIndex = Math.min(numBins - 1, Math.floor((price - minPrice) / binWidth));
    bins[binIndex].count++;
  });

  // Find max count for scaling
  const maxCount = Math.max(...bins.map(b => b.count));

  // Find which bin contains the asking price
  let askingBinIndex = -1;
  if (askingPrice) {
    askingBinIndex = Math.min(numBins - 1, Math.max(0, Math.floor((askingPrice - minPrice) / binWidth)));
  }

  // Render bars
  elements.priceHistogram.innerHTML = bins.map((bin, i) => {
    const height = maxCount > 0 ? (bin.count / maxCount) * 100 : 0;
    const hue = 45 - (i / (numBins - 1)) * 30; // Yellow (45) to Orange (15)
    const isHighlight = i === askingBinIndex;
    const tooltip = `$${formatPriceShort(bin.min)} - $${formatPriceShort(bin.max)}: ${bin.count} об'єктів`;
    return `<div class="histogram-bar ${isHighlight ? 'highlight' : ''}"
                 style="height: ${Math.max(height, 2)}%; background: hsl(${hue}, 90%, 55%);"
                 data-tooltip="${tooltip}"></div>`;
  }).join('');

  // Render labels
  elements.histogramLabels.innerHTML = `
    <span>$${formatPriceShort(minPrice)}</span>
    <span>$${formatPriceShort(maxPrice)}</span>
  `;
}

function formatPriceShort(price) {
  if (price >= 1000000) return (price / 1000000).toFixed(1) + 'M';
  if (price >= 1000) return Math.round(price / 1000) + 'k';
  return price.toString();
}

function displayAnalogs(analogs) {
  if (!analogs) return;

  elements.analogsCount.textContent = `(${analogs.totalCount || 0})`;
  elements.searchRadius.textContent = translations.searchRadius[analogs.searchRadius] || analogs.searchRadius || '-';

  if (!analogs.analogs || !analogs.analogs.length) {
    elements.analogsTableBody.innerHTML = '<tr><td colspan="8" style="text-align: center; padding: 2rem;">Аналоги не знайдені</td></tr>';
    return;
  }

  elements.analogsTableBody.innerHTML = analogs.analogs.map(a => {
    const matchLevel = a.matchScore >= 0.9 ? 'high' : a.matchScore >= 0.8 ? 'medium' : 'low';

    return `
      <tr>
        <td class="address-cell" title="${a.address || ''}">${a.address || '-'}</td>
        <td>${formatPrice(a.price)}</td>
        <td>${a.pricePerMeter ? '$' + a.pricePerMeter : '-'}</td>
        <td>${a.area ? a.area + ' м²' : '-'}</td>
        <td>${a.rooms || '-'}</td>
        <td>${a.floor ? a.floor + (a.totalFloors ? '/' + a.totalFloors : '') : '-'}</td>
        <td><span class="match-badge ${matchLevel}">${Math.round((a.matchScore || 0) * 100)}%</span></td>
        <td>${a.externalUrl ? `<a href="${a.externalUrl}" target="_blank" class="link-btn">Відкрити</a>` : ''}</td>
      </tr>
    `;
  }).join('');
}

function displayReportInfo(data) {
  elements.reportId.textContent = data.reportId || '-';
  elements.generatedAt.textContent = data.generatedAt
    ? new Date(data.generatedAt).toLocaleString('uk-UA')
    : '-';
  const confLabels = { high: 'Висока', medium: 'Середня', low: 'Низька' };
  elements.confidence.textContent = data.liquidity?.confidence
    ? confLabels[data.liquidity.confidence]
    : '-';
}

function formatPrice(value) {
  if (!value) return '-';
  return '$' + Math.round(value).toLocaleString('uk-UA');
}

// Initialize on load
document.addEventListener('DOMContentLoaded', init);
