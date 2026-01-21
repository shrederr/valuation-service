// API Base URL
const API_BASE = '/api/v1/valuation';

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
    pricePerMeter: 'Ціна за м²',
    exposureTime: 'Час експозиції',
    competition: 'Конкуренція',
    location: 'Локація',
    condition: 'Стан',
    format: 'Формат',
    floor: 'Поверх',
    houseType: 'Тип будинку',
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
  const url = params.get('url');

  if (id) {
    elements.objectId.value = id;
    handleSearch();
  } else if (url) {
    switchTab('url');
    elements.externalUrl.value = url;
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

  elements.criteriaGrid.innerHTML = criteria.map(c => {
    const level = c.score >= 7 ? 'high' : c.score >= 5 ? 'medium' : 'low';
    const name = translations.criteria[c.name] || c.name;

    return `
      <div class="criterion-item">
        <div class="criterion-header">
          <span class="criterion-name">${name}</span>
          <span class="criterion-score ${level}">${c.score.toFixed(1)}</span>
        </div>
        <div class="criterion-bar">
          <div class="criterion-bar-fill ${level}" style="width: ${c.score * 10}%"></div>
        </div>
        <div class="criterion-explanation">${c.explanation || ''}</div>
      </div>
    `;
  }).join('');
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
  elements.confidence.textContent = data.confidence
    ? `${Math.round(data.confidence * 100)}%`
    : '-';
}

function formatPrice(value) {
  if (!value) return '-';
  return '$' + Math.round(value).toLocaleString('uk-UA');
}

// Initialize on load
document.addEventListener('DOMContentLoaded', init);
