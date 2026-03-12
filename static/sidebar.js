import { deps, debounce } from './config.js';
import { saveSessionState } from './auth.js';
import { hideAutocomplete } from './autocomplete.js';
import { fetchData } from './data.js';
import { updateChatContextIndicator } from './chat.js';

const { STATE, DOM, CONFIG } = deps;

// This new function will set up the rest of the sidebar logic AFTER the DOM is cached.
export function setupSidebar() {
    if (DOM.sidebarSearch) {
        DOM.sidebarSearch.addEventListener('input', debounce((e) => {
            filterSidebarStocks(e.target.value.trim());
        }, CONFIG.DEBOUNCE_DELAY));
    }

    loadSidebarStocks();

    // Determine initial sidebar state based on screen size and saved state.
    const isMobile = window.matchMedia('(max-width: 768px)').matches;
    if (isMobile) {
        setSidebarCollapsed(true); // Always collapsed on mobile initially
    } else {
        setSidebarCollapsed(STATE.isSidebarCollapsed); // Respect saved state on desktop
    }

    window.matchMedia('(max-width: 768px)').addEventListener('change', (e) => {
        setSidebarCollapsed(e.matches);
    });

    document.addEventListener('click', (e) => {
        if (window.matchMedia('(max-width: 768px)').matches) {
            const mobileToggle = document.querySelector('.mobile-sidebar-toggle');
            if (!DOM.sidebar || !mobileToggle) return;

            if (!STATE.isSidebarCollapsed && !DOM.sidebar.contains(e.target) && !mobileToggle.contains(e.target)) {
                setSidebarCollapsed(true);
            }
        }
    });
}

export function setSidebarCollapsed(collapsed) {
    STATE.isSidebarCollapsed = collapsed;
    saveSessionState();
    const container = document.querySelector('.container');

    DOM.sidebar?.classList.toggle('sidebar-collapsed', collapsed);
    container?.classList.toggle('sidebar-collapsed', collapsed);

    if (DOM.mobileSidebarToggle) {
        DOM.mobileSidebarToggle.innerHTML = collapsed ? '☰' : '✕';
    }
    if (DOM.desktopSidebarToggle) {
        DOM.desktopSidebarToggle.innerHTML = collapsed ? '☰' : '✕';
    }
}

function loadSidebarStocks() {
    if (!STATE.stockDatabase.length) {
        if (DOM.sidebarStocks) {
            DOM.sidebarStocks.innerHTML = `
                <div style="padding: 30px; text-align: center; color: #6b7280;">
                    <div style="font-size: 2rem; margin-bottom: 10px;">📈</div>
                    <div>Loading securities database...</div>
                </div>
            `;
        }
        return;
    }

    const grouped = groupStocksByCategory(STATE.stockDatabase);
    const groupOrder = [
        'mostPopular', 'nifty50', 'usStocks', 'banking', 'tech', 'pharma',
        'auto', 'energy', 'fmcg', 'metals', 'realty', 'midCap', 'smallCap',
        'debtEtf', 'sectorETF', 'goldSilverETF', 'factor', 'liquid', 'bond',
        'thematic', 'bse', 'other'
    ];
    let html = '';

    groupOrder.forEach(groupKey => {
        const stocks = grouped[groupKey];
        if (stocks?.length) {
            html += `
                <div class="sidebar-stock-group">
                    <div class="sidebar-group-header">${getGroupName(groupKey)}</div>
                    ${stocks.map(createSidebarStockItem).join('')}
                </div>
            `;
        }
    });

    if (DOM.sidebarStocks) {
        DOM.sidebarStocks.innerHTML = html;

        DOM.sidebarStocks.querySelectorAll('.sidebar-stock-item').forEach(item => {
            item.addEventListener('click', function() {
                selectStockFromSidebar(this.dataset.symbol);
                if (window.matchMedia('(max-width: 768px)').matches) {
                    setSidebarCollapsed(true);
                }
            });
        });
    }

    if (STATE.currentSymbol) {
        selectStockFromSidebar(STATE.currentSymbol);
    }
}

function filterSidebarStocks(query) {
    const allItems = DOM.sidebarStocks.querySelectorAll('.sidebar-stock-item');
    const groupHeaders = DOM.sidebarStocks.querySelectorAll('.sidebar-group-header');

    if (!query) {
        allItems.forEach(item => item.style.display = 'flex');
        groupHeaders.forEach(header => header.parentElement.style.display = 'block');
        return;
    }

    const lowerQuery = query.toLowerCase();
    allItems.forEach(item => {
        const symbol = item.querySelector('.sidebar-stock-symbol').textContent.toLowerCase();
        const name = item.querySelector('.sidebar-stock-name').textContent.toLowerCase();
        item.style.display = (symbol.includes(lowerQuery) || name.includes(lowerQuery)) ? 'flex' : 'none';
    });

    groupHeaders.forEach(header => {
        const group = header.parentElement;
        const hasVisible = Array.from(group.querySelectorAll('.sidebar-stock-item'))
            .some(item => item.style.display !== 'none');
        group.style.display = hasVisible ? 'block' : 'none';
    });
}

function selectStockFromSidebar(symbol) {
    DOM.symbol.value = symbol;
    hideAutocomplete();

    document.querySelectorAll('.sidebar-stock-item').forEach(item => item.classList.remove('active'));
    const sidebarItem = document.querySelector(`.sidebar-stock-item[data-symbol="${symbol}"]`);
    if (sidebarItem) {
        sidebarItem.classList.add('active');
        sidebarItem.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    }

    if (STATE.currentSymbol !== symbol) {
        STATE.currentSymbol = symbol;
        saveSessionState();
        updateChatContextIndicator(symbol);
        fetchData();
    }
}

// ==================== HELPER FUNCTIONS (MERGED) ====================

function groupStocksByCategory(stocks) {
    const groups = {
        mostPopular: [], nifty50: [], usStocks: [], banking: [], tech: [], pharma: [],
        auto: [], energy: [], fmcg: [], metals: [], realty: [], midCap: [],
        smallCap: [], sectorETF: [], debtEtf: [], goldSilverETF: [], factor: [],
        liquid: [], bond: [], thematic: [], bse: [], other: []
    };

    stocks.forEach(stock => {
        const symbolUpper = stock.symbol.toUpperCase();
        const nameUpper = stock.name.toUpperCase();
        const isStockETF = symbolUpper.endsWith('ETF') || nameUpper.includes('ETF') || nameUpper.includes('EXCHANGE TRADED FUND');

        if (symbolUpper === 'RELIANCE' || symbolUpper === 'TCS' || symbolUpper === 'HDFCBANK' || symbolUpper === 'INFY' || symbolUpper === 'SBIN' || symbolUpper === 'BHARTIARTL') {
            groups.mostPopular.push(stock);
        }

        if (symbolUpper.includes('NIFTY') || symbolUpper.includes('BANKNIFTY')) {
            groups.nifty50.push(stock);
        } else if (symbolUpper.length <= 4 && symbolUpper.match(/^[A-Z]+$/)) {
            groups.usStocks.push(stock);
        } else if (nameUpper.includes('BANK') || nameUpper.includes('FINANCE') || nameUpper.includes('NBFC')) {
            (isStockETF ? groups.sectorETF : groups.banking).push(stock);
        } else if (nameUpper.includes('TECH') || nameUpper.includes('INFO') || nameUpper.includes('SOFTWARE')) {
            (isStockETF ? groups.sectorETF : groups.tech).push(stock);
        } else if (nameUpper.includes('PHARMA') || nameUpper.includes('HEALTH') || nameUpper.includes('DRUG')) {
            (isStockETF ? groups.sectorETF : groups.pharma).push(stock);
        } else if (nameUpper.includes('AUTO') || nameUpper.includes('MOTOR') || nameUpper.includes('VEHICLE')) {
            (isStockETF ? groups.sectorETF : groups.auto).push(stock);
        } else if (nameUpper.includes('ENERGY') || nameUpper.includes('POWER') || nameUpper.includes('OIL')) {
            (isStockETF ? groups.sectorETF : groups.energy).push(stock);
        } else if (!isStockETF && (nameUpper.includes('CONSUMER') || nameUpper.includes('FMCG'))) {
            groups.fmcg.push(stock);
        } else if (nameUpper.includes('METAL') || nameUpper.includes('STEEL') || nameUpper.includes('MINING')) {
            (isStockETF ? groups.sectorETF : groups.metals).push(stock);
        } else if (!isStockETF && nameUpper.includes('REAL')) {
            groups.realty.push(stock);
        } else if (nameUpper.includes('MIDCAP')) {
            (isStockETF ? groups.sectorETF : groups.midCap).push(stock);
        } else if (nameUpper.includes('SMALLCAP')) {
            (isStockETF ? groups.sectorETF : groups.smallCap).push(stock);
        } else if (isStockETF && (symbolUpper.includes('GOLD') || symbolUpper.includes('SILVER') || nameUpper.includes('GOLD') || nameUpper.includes('SILVER'))) {
            groups.goldSilverETF.push(stock);
        } else if (isStockETF && (nameUpper.includes('FACTOR') || nameUpper.includes('SMART BETA'))) {
            groups.factor.push(stock);
        } else if (isStockETF && (nameUpper.includes('LIQUID') || nameUpper.includes('DEBT'))) {
            groups.liquid.push(stock);
        } else if (isStockETF && (nameUpper.includes('BOND') || nameUpper.includes('G-SEC'))) {
            groups.bond.push(stock);
        } else if (isStockETF && (nameUpper.includes('THEMATIC'))) {
            groups.thematic.push(stock);
        } else if (symbolUpper.endsWith('BSE') || nameUpper.includes('BSE')) {
            groups.bse.push(stock);
        } else if (isStockETF) {
            groups.sectorETF.push(stock);
        } else {
            groups.other.push(stock);
        }
    });

    groups.nifty50.sort((a, b) => a.symbol.localeCompare(b.symbol));
    groups.mostPopular.sort((a, b) => a.symbol.localeCompare(b.symbol));

    return groups;
}

function getGroupName(groupKey) {
    const names = {
        mostPopular: '🔥 Most Popular', nifty50: '🇮🇳 Nifty 50 & Indices',
        banking: '🏦 Banking & Finance', tech: '💻 Technology (IT/Software)',
        pharma: '💊 Pharma & Healthcare', auto: '🚗 Automobile', energy: '⚡ Energy',
        fmcg: '🛒 FMCG & Consumer Goods', metals: '⚙️ Metals & Mining',
        realty: '🏙️ Realty', midCap: '🟡 MidCap', smallCap: '🔵 SmallCap',
        sectorETF: '📊 Sector ETFs', debtEtf: '💸 Debt ETFs', goldSilverETF: '🪙 Gold & Silver ETFs',
        factor: '🎯 Factor & Smart Beta', liquid: '💧 Liquid & Debt ETFs', bond: '📋 Bond & G-Sec ETFs',
        thematic: '🎨 Thematic ETFs', bse: '📈 BSE Listed', usStocks: '🇺🇸 US Stocks',
        other: '📂 Other Securities'
    };
    return names[groupKey] || groupKey;
}

function createSidebarStockItem(stock) {
    return `
        <div class="sidebar-stock-item tooltip" data-symbol="${stock.symbol}">
            <div class="sidebar-stock-symbol">${stock.symbol}</div>
            <div class="sidebar-stock-name">${stock.name}</div>
            <span class="tooltip-text">
                <span class="tooltip-title">${stock.symbol}</span>
                <span class="tooltip-row">
                    <span class="tooltip-label">Company:</span>
                    <span class="tooltip-value">${stock.name}</span>
                </span>
                <span class="tooltip-row">
                    <span class="tooltip-label">Sector:</span>
                    <span class="tooltip-value">Click to view analysis</span>
                </span>
            </span>
        </div>
    `;
}
