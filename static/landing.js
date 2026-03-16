// ==================== LANDING PAGE FUNCTIONALITY ====================

const DEMO_STOCKS = [
  { symbol: 'RELIANCE', name: 'Reliance Industries Ltd', ns: 'RELIANCE.NS' },
  { symbol: 'TCS', name: 'Tata Consultancy Services Ltd', ns: 'TCS.NS' },
  { symbol: 'INFY', name: 'Infosys Ltd', ns: 'INFY.NS' },
  { symbol: 'HDFCBANK', name: 'HDFC Bank Ltd', ns: 'HDFCBANK.NS' },
  { symbol: 'ICICIBANK', name: 'ICICI Bank Ltd', ns: 'ICICIBANK.NS' },
  { symbol: 'SBIN', name: 'State Bank of India', ns: 'SBIN.NS' },
  { symbol: 'WIPRO', name: 'Wipro Ltd', ns: 'WIPRO.NS' },
  { symbol: 'BHARTIARTL', name: 'Bharti Airtel Ltd', ns: 'BHARTIARTL.NS' },
  { symbol: 'ITC', name: 'ITC Ltd', ns: 'ITC.NS' },
  { symbol: 'KOTAKBANK', name: 'Kotak Mahindra Bank Ltd', ns: 'KOTAKBANK.NS' },
  { symbol: 'ADANIPORTS', name: 'Adani Ports and SEZ Ltd', ns: 'ADANIPORTS.NS' },
  { symbol: 'ASIANPAINT', name: 'Asian Paints Ltd', ns: 'ASIANPAINT.NS' },
  { symbol: 'AXISBANK', name: 'Axis Bank Ltd', ns: 'AXISBANK.NS' },
  { symbol: 'BAJFINANCE', name: 'Bajaj Finance Ltd', ns: 'BAJFINANCE.NS' },
  { symbol: 'MARUTI', name: 'Maruti Suzuki India Ltd', ns: 'MARUTI.NS' },
  { symbol: 'SUNPHARMA', name: 'Sun Pharmaceutical Industries Ltd', ns: 'SUNPHARMA.NS' },
  { symbol: 'TITAN', name: 'Titan Company Ltd', ns: 'TITAN.NS' },
  { symbol: 'ULTRACEMCO', name: 'UltraTech Cement Ltd', ns: 'ULTRACEMCO.NS' },
  { symbol: 'HINDUNILVR', name: 'Hindustan Unilever Ltd', ns: 'HINDUNILVR.NS' },
  { symbol: 'NESTLEIND', name: 'Nestle India Ltd', ns: 'NESTLEIND.NS' },
  { symbol: 'LTIM', name: 'LTIMindtree Ltd', ns: 'LTIM.NS' },
  { symbol: 'TECHM', name: 'Tech Mahindra Ltd', ns: 'TECHM.NS' },
  { symbol: 'HCLTECH', name: 'HCL Technologies Ltd', ns: 'HCLTECH.NS' },
  { symbol: 'ADANI', name: 'Adani Enterprises Ltd', ns: 'ADANIENT.NS' },
  { symbol: 'AMBUJCEM', name: 'Ambuja Cements Ltd', ns: 'AMBUJCEM.NS' }
];

const AI_ANALYSES = {
  bullish: [
    "Strong bullish momentum with RSI at 65. MACD shows positive crossover. Volume indicates sustained interest.",
    "Bullish engulfing pattern formed on daily chart. Moving averages aligned for potential upmove.",
    "Breaking out above resistance with strong volume. Watch for continuation toward next resistance level."
  ],
  bearish: [
    "Bearish divergence on RSI. MACD showing negative crossover. Consider trimming positions.",
    "Breaking below support with increased selling pressure. Watch for further downside.",
    "Technical indicators suggest bearish momentum. Price testing key support levels."
  ],
  neutral: [
    "Consolidating near support. AI suggests a breakout opportunity if volume increases above average.",
    "Sideways movement expected. Consider waiting for clear direction before entering position.",
    "Range-bound movement. Watch for breakout above ₹{resistance} or breakdown below ₹{support}."
  ]
};

let selectedDemoIndex = -1;
let stockCache = new Map();
let cacheExpiry = new Map();
const CACHE_DURATION = 60000;

async function fetchStockData(symbol) {
  const cacheKey = symbol.toUpperCase();
  
  if (stockCache.has(cacheKey) && cacheExpiry.get(cacheKey) > Date.now()) {
    return stockCache.get(cacheKey);
  }

  const stockInfo = DEMO_STOCKS.find(s => s.symbol === cacheKey || s.ns === cacheKey);
  const nsSymbol = stockInfo ? stockInfo.ns : `${cacheKey}.NS`;
  
  try {
    const response = await fetch(
      `https://query1.finance.yahoo.com/v8/finance/chart/${nsSymbol}?interval=1d&range=5d`,
      {
        headers: {
          'Accept': 'application/json'
        }
      }
    );
    
    if (!response.ok) throw new Error('Failed to fetch');
    
    const data = await response.json();
    const result = data.chart?.result?.[0];
    
    if (!result) throw new Error('No data');
    
    const meta = result.meta;
    const quote = result.indicators?.quote?.[0];
    const currentPrice = meta.regularMarketPrice || 0;
    const prevClose = meta.previousClose || meta.chartPreviousClose || currentPrice;
    const change = currentPrice - prevClose;
    const changePercent = prevClose > 0 ? (change / prevClose) * 100 : 0;
    
    const closePrices = quote?.close?.filter(c => c !== null) || [];
    const rsi = calculateRSI(closePrices);
    const macd = calculateMACD(closePrices);
    
    const stockData = {
      symbol: cacheKey,
      name: stockInfo?.name || nsSymbol,
      price: currentPrice,
      prevClose: prevClose,
      change: change,
      changePercent: changePercent,
      positive: change >= 0,
      rsi: rsi,
      macd: macd,
      high: meta.regularMarketDayHigh || currentPrice,
      low: meta.regularMarketDayLow || currentPrice,
      volume: meta.regularMarketVolume || 0
    };
    
    stockCache.set(cacheKey, stockData);
    cacheExpiry.set(cacheKey, Date.now() + CACHE_DURATION);
    
    return stockData;
  } catch (error) {
    console.error('Error fetching stock data:', error);
    return null;
  }
}

function calculateRSI(prices, period = 14) {
  if (prices.length < period + 1) return 50;
  
  let gains = 0, losses = 0;
  for (let i = prices.length - period; i < prices.length; i++) {
    const change = prices[i] - prices[i - 1];
    if (change > 0) gains += change;
    else losses -= change;
  }
  
  const avgGain = gains / period;
  const avgLoss = losses / period;
  
  if (avgLoss === 0) return 100;
  const rs = avgGain / avgLoss;
  return 100 - (100 / (1 + rs));
}

function calculateMACD(prices, fast = 12, slow = 26, signal = 9) {
  if (prices.length < slow) return { macd: 0, signal: 0, histogram: 0 };
  
  const ema = (arr, period) => {
    const k = 2 / (period + 1);
    let emaArray = [arr[0]];
    for (let i = 1; i < arr.length; i++) {
      emaArray.push(arr[i] * k + emaArray[i - 1] * (1 - k));
    }
    return emaArray;
  };
  
  const fastEMA = ema(prices, fast);
  const slowEMA = ema(prices, slow);
  const macdLine = fastEMA.map((f, i) => f - slowEMA[i]);
  const signalLine = ema(macdLine.slice(-9), signal);
  const macd = macdLine[macdLine.length - 1];
  const sig = signalLine[signalLine.length - 1];
  
  return { macd, signal: sig, histogram: macd - sig };
}

function generateAIAnalysis(stockData) {
  const { rsi, macd, positive, price, changePercent } = stockData;
  let sentiment = 'neutral';
  let factors = [];
  
  if (rsi > 70) {
    factors.push('RSI at ' + rsi.toFixed(0) + ' indicates overbought conditions');
  } else if (rsi < 30) {
    factors.push('RSI at ' + rsi.toFixed(0) + ' suggests oversold');
  } else if (rsi > 55) {
    factors.push('RSI at ' + rsi.toFixed(0) + ' shows bullish momentum');
  } else if (rsi < 45) {
    factors.push('RSI at ' + rsi.toFixed(0) + ' indicates bearish momentum');
  }
  
  if (macd.histogram > 0) {
    factors.push('MACD bullish crossover');
  } else if (macd.histogram < 0) {
    factors.push('MACD showing negative momentum');
  }
  
  if (Math.abs(changePercent) > 2) {
    factors.push(changePercent > 0 ? 'Strong buying pressure today' : 'Significant selling pressure');
  }
  
  if (factors.length >= 2) sentiment = positive ? 'bullish' : 'bearish';
  
  const analysis = AI_ANALYSES[sentiment][Math.floor(Math.random() * AI_ANALYSES[sentiment].length)];
  const resistance = (price * 1.03).toFixed(2);
  const support = (price * 0.97).toFixed(2);
  
  return analysis.replace('{resistance}', resistance).replace('{support}', support) + 
    ' RSI: ' + rsi.toFixed(0) + '. ' +
    (macd.histogram > 0 ? 'MACD bullish.' : 'MACD bearish.');
}

function initDemoSearch() {
  const demoInput = document.getElementById('demo-search-input');
  const demoAutocomplete = document.getElementById('demo-autocomplete');
  const searchBtn = document.querySelector('.search-demo .search-btn');

  if (!demoInput || !demoAutocomplete || !searchBtn) return;

  demoInput.addEventListener('input', handleDemoInput);
  demoInput.addEventListener('keydown', handleDemoKeydown);
  searchBtn.addEventListener('click', handleDemoSearch);
  
  document.addEventListener('click', (e) => {
    if (!e.target.closest('.search-demo')) {
      demoAutocomplete.classList.remove('active');
    }
  });
}

function handleDemoInput(e) {
  const demoInput = e.target;
  const demoAutocomplete = document.getElementById('demo-autocomplete');
  const query = demoInput.value.trim().toUpperCase();

  selectedDemoIndex = -1;

  if (!query) {
    demoAutocomplete.classList.remove('active');
    return;
  }

  const matches = DEMO_STOCKS.filter(s => 
    s.symbol.toLowerCase().includes(query.toLowerCase()) || 
    s.name.toLowerCase().includes(query.toLowerCase())
  ).slice(0, 6);

  if (matches.length > 0) {
    demoAutocomplete.innerHTML = matches.map((s, i) => `
      <div class="autocomplete-item" data-symbol="${s.symbol}">
        <div class="symbol">${s.symbol}</div>
        <div class="name">${s.name}</div>
      </div>
    `).join('');
    
    demoAutocomplete.querySelectorAll('.autocomplete-item').forEach(item => {
      item.addEventListener('click', () => {
        demoInput.value = item.dataset.symbol;
        demoAutocomplete.classList.remove('active');
        showDemoResult(item.dataset.symbol);
      });
    });
    
    demoAutocomplete.classList.add('active');
  } else {
    demoAutocomplete.classList.remove('active');
  }
}

function handleDemoKeydown(e) {
  const demoAutocomplete = document.getElementById('demo-autocomplete');
  const items = demoAutocomplete.querySelectorAll('.autocomplete-item');
  if (!items.length) return;

  if (e.key === 'ArrowDown') {
    e.preventDefault();
    selectedDemoIndex = Math.min(selectedDemoIndex + 1, items.length - 1);
    updateDemoSelection(items);
  } else if (e.key === 'ArrowUp') {
    e.preventDefault();
    selectedDemoIndex = Math.max(selectedDemoIndex - 1, 0);
    updateDemoSelection(items);
  } else if (e.key === 'Enter') {
    e.preventDefault();
    const demoInput = document.getElementById('demo-search-input');
    if (selectedDemoIndex >= 0) {
      items[selectedDemoIndex].click();
    } else if (demoInput.value.trim()) {
      showDemoResult(demoInput.value.trim().toUpperCase());
    }
  } else if (e.key === 'Escape') {
    demoAutocomplete.classList.remove('active');
  }
}

function updateDemoSelection(items) {
  items.forEach((item, i) => {
    item.style.background = i === selectedDemoIndex ? 'rgba(59, 130, 246, 0.2)' : '';
  });
  if (items[selectedDemoIndex]) {
    items[selectedDemoIndex].scrollIntoView({ block: 'nearest' });
  }
}

function handleDemoSearch() {
  const demoInput = document.getElementById('demo-search-input');
  const demoAutocomplete = document.getElementById('demo-autocomplete');
  
  if (demoInput.value.trim()) {
    demoAutocomplete.classList.remove('active');
    showDemoResult(demoInput.value.trim().toUpperCase());
  }
}

async function showDemoResult(symbol) {
  const demoResult = document.getElementById('demo-result');
  const loadingEl = document.getElementById('demo-loading');
  const contentEl = document.getElementById('demo-content');
  
  const stock = DEMO_STOCKS.find(s => s.symbol === symbol) || { symbol, name: symbol + ' Ltd' };
  
  demoResult.classList.add('active');
  if (loadingEl) loadingEl.style.display = 'flex';
  if (contentEl) contentEl.style.display = 'none';
  
  try {
    // Use the API_BASE_URL from config
    const apiBase = window.API_BASE_URL || '/api';
    const response = await fetch(`${apiBase}/demo/search?symbol=${encodeURIComponent(symbol)}`);
    console.log('Demo search response:', response.status, response.statusText);
    
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }
    
    const data = await response.json();
    
    if (loadingEl) loadingEl.style.display = 'none';
    if (contentEl) contentEl.style.display = 'block';
    
    if (data.error) {
      document.getElementById('demo-symbol').textContent = stock.symbol;
      document.getElementById('demo-name').textContent = data.error;
      document.getElementById('demo-price').textContent = '--';
      document.getElementById('demo-change').textContent = 'Unable to fetch data';
      document.getElementById('demo-analysis').textContent = 'Please try again later or sign in for full access.';
      return;
    }
    
    document.getElementById('demo-symbol').textContent = data.symbol;
    document.getElementById('demo-name').textContent = data.name;
    document.getElementById('demo-price').textContent = data.price;
    document.getElementById('demo-change').textContent = data.change_percent;
    
    const priceEl = document.getElementById('demo-price');
    const changeEl = document.getElementById('demo-change');
    
    if (data.change_percent.startsWith('+')) {
      priceEl.className = 'stock-price';
      changeEl.className = 'stock-change';
    } else {
      priceEl.className = 'stock-price negative';
      changeEl.className = 'stock-change negative';
    }

    document.getElementById('demo-analysis').textContent = data.analysis;
    
  } catch (error) {
    console.error('Error fetching demo data:', error);
    
    if (loadingEl) loadingEl.style.display = 'none';
    if (contentEl) contentEl.style.display = 'block';
    
    document.getElementById('demo-symbol').textContent = stock.symbol;
    document.getElementById('demo-name').textContent = 'Connection error';
    document.getElementById('demo-price').textContent = '--';
    document.getElementById('demo-change').textContent = 'Unable to fetch data';
    document.getElementById('demo-analysis').textContent = 'Please try again later or sign in for full access.';
  }
}

function initSmoothScroll() {
  document.querySelectorAll('a[href^="#"]').forEach(anchor => {
    anchor.addEventListener('click', function(e) {
      e.preventDefault();
      const target = document.querySelector(this.getAttribute('href'));
      if (target) {
        target.scrollIntoView({ behavior: 'smooth', block: 'start' });
      }
    });
  });
}

function initLoginButtons() {
  const headerLoginBtn = document.getElementById('header-login-btn');
  const demoLoginBtn = document.getElementById('demo-login-btn');

  const scrollToLogin = () => {
    const demoResult = document.getElementById('demo-result');
    if (demoResult) {
      demoResult.scrollIntoView({ behavior: 'smooth', block: 'center' });
    }
  };

  if (headerLoginBtn) headerLoginBtn.addEventListener('click', scrollToLogin);
  if (demoLoginBtn) {
    import('./auth.js').then(module => {
      demoLoginBtn.addEventListener('click', module.handleGoogleLogin);
    });
  }
}

document.addEventListener('DOMContentLoaded', () => {
  initDemoSearch();
  initSmoothScroll();
  initLoginButtons();
});