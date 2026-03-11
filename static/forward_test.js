import { CONFIG } from './config.js';
import { showNotification } from './notifications.js';

// ==================== FORWARD / PAPER TESTING MODULE ====================
let ftChart = null;
let ftCandles = [];
let ftIndex = 0;
let ftTotalCandles = 0;
let ftSelectedDate = null;

// Trading state
let ftCash = 100000;
let ftInitialCash = 100000;
let ftPositionSize = 25;     // % of capital per trade
let ftShares = 0;
let ftEntryPrice = 0;
let ftTrades = [];

// ── Public entry point ──
export function openForwardTestModal() {
    const modal = document.getElementById('forward-test-modal');
    if (!modal) return;
    modal.showModal();

    const symbol = window.currentBacktestData?.symbol || '';
    const label = document.getElementById('ft-symbol-label');
    if (label) {
        label.textContent = symbol
            ? `Paper trading ${symbol}`
            : 'Simulate trading on historical data';
    }

    showFTSetup();
    populateFTDateGrid();
    populateFTTimeDropdowns();
    bindFTSetupEvents();
    bindFTTradingEvents();
}
window.openForwardTestModal = openForwardTestModal;

// ── Setup / Trading / Results toggle ──
function showFTSetup() {
    el('ft-setup').style.display = '';
    el('ft-trading').style.display = 'none';
    el('ft-results').style.display = 'none';
    resetFTState();
}

function showFTTrading() {
    el('ft-setup').style.display = 'none';
    el('ft-trading').style.display = '';
    el('ft-results').style.display = 'none';
}

function showFTResults() {
    el('ft-setup').style.display = 'none';
    el('ft-trading').style.display = 'none';
    el('ft-results').style.display = '';
    renderFTResults();
}

function el(id) { return document.getElementById(id) || document.createElement('div'); }

// ── Date Grid (reuse replay pattern) ──
function populateFTDateGrid() {
    const grid = el('ft-date-grid');
    grid.innerHTML = '';
    const today = new Date();
    const dates = [];
    for (let i = 31; i <= 90; i++) {
        const d = new Date(today);
        d.setDate(d.getDate() - i);
        if (d.getDay() === 0 || d.getDay() === 6) continue;
        dates.push(d);
        if (dates.length >= 30) break;
    }

    const mn = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    const dn = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];

    dates.forEach(d => {
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'replay-date-btn';
        btn.innerHTML = `<span class="rdb-day">${dn[d.getDay()]}</span><span class="rdb-num">${d.getDate()}</span><span class="rdb-month">${mn[d.getMonth()]}</span>`;
        btn.dataset.date = d.toISOString().split('T')[0];
        btn.addEventListener('click', () => {
            grid.querySelectorAll('.replay-date-btn').forEach(b => b.classList.remove('selected'));
            btn.classList.add('selected');
            ftSelectedDate = btn.dataset.date;
            updateFTDuration();
        });
        grid.appendChild(btn);
    });
}

function populateFTTimeDropdowns() {
    ['ft-hour-start', 'ft-hour-end'].forEach(id => {
        const sel = el(id);
        sel.innerHTML = '';
        for (let h = 9; h <= 15; h++) {
            const opt = document.createElement('option');
            opt.value = h; opt.textContent = String(h).padStart(2, '0');
            sel.appendChild(opt);
        }
    });
    ['ft-min-start', 'ft-min-end'].forEach(id => {
        const sel = el(id);
        sel.innerHTML = '';
        for (let m = 0; m < 60; m++) {
            const opt = document.createElement('option');
            opt.value = m; opt.textContent = String(m).padStart(2, '0');
            sel.appendChild(opt);
        }
    });
    el('ft-hour-start').value = '10';
    el('ft-min-start').value = '0';
    el('ft-hour-end').value = '10';
    el('ft-min-end').value = '30';

    ['ft-hour-start', 'ft-hour-end', 'ft-min-start', 'ft-min-end'].forEach(id => {
        el(id).addEventListener('change', updateFTDuration);
    });
}

function getFTTimes() {
    return {
        hs: parseInt(el('ft-hour-start').value || '10'),
        ms: parseInt(el('ft-min-start').value || '0'),
        he: parseInt(el('ft-hour-end').value || '10'),
        me: parseInt(el('ft-min-end').value || '30'),
    };
}

function updateFTDuration() {
    const badge = el('ft-duration-badge');
    const btn = el('ft-launch-btn');
    const { hs, ms, he, me } = getFTTimes();
    const diff = (he * 60 + me) - (hs * 60 + ms);

    if (diff <= 0 || diff > 60) {
        badge.textContent = diff <= 0 ? 'Invalid range' : `${diff} min (max 60)`;
        badge.classList.add('invalid');
        btn.disabled = true;
        return;
    }
    badge.textContent = `${diff} min · ${diff} candles`;
    badge.classList.remove('invalid');
    btn.disabled = !ftSelectedDate;
}

// ── Setup Events ──
function bindFTSetupEvents() {
    // Position sizing pills
    document.querySelectorAll('.ft-size-pill').forEach(pill => {
        pill.onclick = () => {
            document.querySelectorAll('.ft-size-pill').forEach(p => p.classList.remove('active'));
            pill.classList.add('active');
            ftPositionSize = parseInt(pill.dataset.pct);
        };
    });

    const launchBtn = el('ft-launch-btn');
    const nb = launchBtn.cloneNode(true);
    launchBtn.parentNode.replaceChild(nb, launchBtn);
    nb.addEventListener('click', handleFTLaunch);

    const closeBtn = el('ft-close-btn');
    const nc = closeBtn.cloneNode(true);
    closeBtn.parentNode.replaceChild(nc, closeBtn);
    nc.addEventListener('click', () => {
        resetFTState();
        document.getElementById('forward-test-modal')?.close();
    });
}

async function handleFTLaunch() {
    const symbol = window.currentBacktestData?.symbol;
    if (!symbol) { showNotification('Run a backtest first.', 'error'); return; }
    if (!ftSelectedDate) { showNotification('Select a date.', 'error'); return; }

    // Read capital
    ftInitialCash = parseFloat(el('ft-capital').value) || 100000;
    ftCash = ftInitialCash;

    const { hs, ms, he, me } = getFTTimes();
    const startISO = `${ftSelectedDate}T${String(hs).padStart(2,'0')}:${String(ms).padStart(2,'0')}:00`;
    const endISO = `${ftSelectedDate}T${String(he).padStart(2,'0')}:${String(me).padStart(2,'0')}:00`;

    setFTStatus('Loading candles...');
    const btn = el('ft-launch-btn');
    btn.disabled = true; btn.textContent = 'Loading...';

    try {
        const url = `${CONFIG.API_BASE_URL}/replay/candles?symbol=${encodeURIComponent(symbol)}&start=${encodeURIComponent(startISO)}&end=${encodeURIComponent(endISO)}`;
        const res = await fetch(url, { credentials: 'include' });
        const data = await res.json();

        if (!res.ok || data.error) throw new Error(data.error || `Server ${res.status}`);

        ftCandles = data.candles || [];
        ftTotalCandles = data.total || ftCandles.length;
        if (ftCandles.length === 0) throw new Error('No candle data for this window.');

        ftIndex = 0;
        ftTrades = [];
        ftShares = 0;
        ftEntryPrice = 0;

        showFTTrading();
        initFTChart(symbol);
        advanceFTCandle(); // Show first candle
        updateFTPortfolio();
        setFTStatus('Make your first trade!');
    } catch (err) {
        setFTStatus('');
        showNotification(err.message, 'error');
    } finally {
        btn.disabled = false;
        btn.innerHTML = '<span>🎯</span> Start Paper Trading';
    }
}

// ── Chart ──
function initFTChart(symbol) {
    const canvas = el('ft-chart');
    if (ftChart) { ftChart.destroy(); ftChart = null; }

    ftChart = new Chart(canvas.getContext('2d'), {
        type: 'line',
        data: {
            labels: [],
            datasets: [
                { label: `${symbol} Close`, data: [], borderColor: '#00D4C8', backgroundColor: 'rgba(0,212,200,0.08)', borderWidth: 2, fill: true, tension: 0.2, pointRadius: 0, pointHoverRadius: 5, pointHoverBackgroundColor: '#00D4C8' },
                { label: 'High', data: [], borderColor: 'rgba(74,222,128,0.4)', borderWidth: 1, borderDash: [3,3], fill: false, tension: 0.2, pointRadius: 0 },
                { label: 'Low', data: [], borderColor: 'rgba(248,113,113,0.4)', borderWidth: 1, borderDash: [3,3], fill: false, tension: 0.2, pointRadius: 0 }
            ]
        },
        options: {
            responsive: true, maintainAspectRatio: false, animation: { duration: 120 },
            plugins: {
                legend: { display: false },
                tooltip: { mode: 'index', intersect: false },
                annotation: {
                    annotations: {} // Will hold buy/sell markers
                }
            },
            scales: {
                x: { ticks: { color: 'rgba(255,255,255,0.5)', maxTicksLimit: 10, font: { size: 11 } }, grid: { color: 'rgba(255,255,255,0.04)' } },
                y: { position: 'right', ticks: { color: 'rgba(255,255,255,0.5)', font: { size: 11 }, callback: v => '₹' + v.toFixed(0) }, grid: { color: 'rgba(255,255,255,0.06)' } }
            },
            interaction: { mode: 'nearest', axis: 'x', intersect: false }
        }
    });
}

function renderFTChart() {
    if (!ftChart) return;
    const slice = ftCandles.slice(0, ftIndex);

    ftChart.data.labels = slice.map(c => fmtTime(c));
    ftChart.data.datasets[0].data = slice.map(c => c.Close ?? c.close);
    ftChart.data.datasets[1].data = slice.map(c => c.High ?? c.high);
    ftChart.data.datasets[2].data = slice.map(c => c.Low ?? c.low);
    ftChart.update('none');

    // Update candle info
    if (slice.length > 0) {
        const last = slice[slice.length - 1];
        el('ft-open').textContent = '₹' + ((last.Open ?? last.open) || 0).toFixed(2);
        el('ft-high').textContent = '₹' + ((last.High ?? last.high) || 0).toFixed(2);
        el('ft-low').textContent = '₹' + ((last.Low ?? last.low) || 0).toFixed(2);
        el('ft-close').textContent = '₹' + ((last.Close ?? last.close) || 0).toFixed(2);
        el('ft-time').textContent = fmtTime(last);
    }

    // Progress
    el('ft-candle-counter').textContent = `${ftIndex} / ${ftTotalCandles}`;
    const pct = ftTotalCandles > 0 ? (ftIndex / ftTotalCandles) * 100 : 0;
    el('ft-progress-fill').style.width = pct + '%';
    el('ft-progress-thumb').style.left = pct + '%';

    if (slice.length > 0) {
        el('ft-time-label').textContent = `${fmtTime(slice[0])} → ${fmtTime(slice[slice.length - 1])}`;
    }
}

function advanceFTCandle() {
    if (ftIndex < ftCandles.length) {
        ftIndex++;
        renderFTChart();
        updateFTPortfolio();
    }

    // Check if we reached the end
    if (ftIndex >= ftCandles.length) {
        // Force sell if holding
        if (ftShares > 0) {
            const last = ftCandles[ftCandles.length - 1];
            const exitPrice = last.Close ?? last.close;
            const pnl = (exitPrice - ftEntryPrice) * ftShares;
            ftCash += exitPrice * ftShares;
            ftTrades.push({
                type: 'SELL (auto)',
                price: exitPrice,
                shares: ftShares,
                pnl: pnl,
                time: fmtTime(last),
                candleIdx: ftIndex
            });
            ftShares = 0;
            ftEntryPrice = 0;
        }
        updateFTPortfolio();
        showFTResults();
    }
}

// ── Trading Actions ──
function bindFTTradingEvents() {
    el('ft-btn-buy').onclick = handleFTBuy;
    el('ft-btn-sell').onclick = handleFTSell;
    el('ft-btn-skip').onclick = handleFTSkip;

    // Results buttons
    const restartBtn = el('ft-btn-restart');
    if (restartBtn) restartBtn.onclick = () => showFTSetup();

    const closeResBtn = el('ft-btn-close-results');
    if (closeResBtn) closeResBtn.onclick = () => {
        resetFTState();
        document.getElementById('forward-test-modal')?.close();
    };
}

function handleFTBuy() {
    if (ftShares > 0) { showNotification('Already holding a position. Sell first.', 'warning'); return; }
    if (ftIndex < 1 || ftIndex > ftCandles.length) return;

    const candle = ftCandles[ftIndex - 1];
    const price = candle.Close ?? candle.close;
    const capitalToUse = ftCash * (ftPositionSize / 100);
    const shares = Math.floor(capitalToUse / price);

    if (shares < 1) { showNotification('Not enough capital to buy.', 'error'); return; }

    ftShares = shares;
    ftEntryPrice = price;
    ftCash -= shares * price;

    ftTrades.push({
        type: 'BUY',
        price: price,
        shares: shares,
        pnl: null,
        time: fmtTime(candle),
        candleIdx: ftIndex
    });

    updateFTPortfolio();
    renderFTTradeLog();
    advanceFTCandle();
}

function handleFTSell() {
    if (ftShares <= 0) { showNotification('No position to sell.', 'warning'); return; }
    if (ftIndex < 1) return;

    const candle = ftCandles[ftIndex - 1];
    const exitPrice = candle.Close ?? candle.close;
    const pnl = (exitPrice - ftEntryPrice) * ftShares;
    ftCash += exitPrice * ftShares;

    ftTrades.push({
        type: 'SELL',
        price: exitPrice,
        shares: ftShares,
        pnl: pnl,
        time: fmtTime(candle),
        candleIdx: ftIndex
    });

    ftShares = 0;
    ftEntryPrice = 0;

    updateFTPortfolio();
    renderFTTradeLog();
    advanceFTCandle();
}

function handleFTSkip() {
    advanceFTCandle();
}

// ── Portfolio display ──
function updateFTPortfolio() {
    el('ft-cash').textContent = '₹' + ftCash.toLocaleString('en-IN', { maximumFractionDigits: 0 });

    if (ftShares > 0 && ftIndex > 0 && ftIndex <= ftCandles.length) {
        const currentPrice = ftCandles[ftIndex - 1].Close ?? ftCandles[ftIndex - 1].close;
        const unrealised = (currentPrice - ftEntryPrice) * ftShares;
        const cls = unrealised >= 0 ? 'positive' : 'negative';

        el('ft-position').textContent = `${ftShares} shares @ ₹${ftEntryPrice.toFixed(2)}`;
        el('ft-unrealised').textContent = `${unrealised >= 0 ? '+' : ''}₹${unrealised.toFixed(0)}`;
        el('ft-unrealised').className = 'ft-stat-value ' + cls;
        el('ft-btn-sell').disabled = false;
        el('ft-btn-buy').disabled = true;
    } else {
        el('ft-position').textContent = 'None';
        el('ft-unrealised').textContent = '₹0';
        el('ft-unrealised').className = 'ft-stat-value';
        el('ft-btn-sell').disabled = true;
        el('ft-btn-buy').disabled = false;
    }

    // Total P&L
    const realisedPnl = ftTrades.filter(t => t.pnl != null).reduce((sum, t) => sum + t.pnl, 0);
    let unrealisedPnl = 0;
    if (ftShares > 0 && ftIndex > 0) {
        const cp = ftCandles[ftIndex - 1].Close ?? ftCandles[ftIndex - 1].close;
        unrealisedPnl = (cp - ftEntryPrice) * ftShares;
    }
    const totalPnl = realisedPnl + unrealisedPnl;
    el('ft-total-pnl').textContent = `${totalPnl >= 0 ? '+' : ''}₹${totalPnl.toFixed(0)}`;
    el('ft-total-pnl').className = 'ft-stat-value ' + (totalPnl >= 0 ? 'positive' : 'negative');
}

function renderFTTradeLog() {
    const list = el('ft-trades-list');
    if (ftTrades.length === 0) {
        list.innerHTML = '<p class="ft-no-trades">No trades yet. Start trading!</p>';
        return;
    }

    list.innerHTML = ftTrades.map((t, i) => {
        const isBuy = t.type === 'BUY';
        const pnlStr = t.pnl != null ? `${t.pnl >= 0 ? '+' : ''}₹${t.pnl.toFixed(0)}` : '';
        const cls = isBuy ? 'ft-log-buy' : (t.pnl >= 0 ? 'ft-log-win' : 'ft-log-loss');
        return `<div class="ft-log-entry ${cls}">
            <span class="ft-log-type">${t.type}</span>
            <span class="ft-log-detail">${t.shares} shares @ ₹${t.price.toFixed(2)}</span>
            <span class="ft-log-time">${t.time}</span>
            ${pnlStr ? `<span class="ft-log-pnl">${pnlStr}</span>` : ''}
        </div>`;
    }).join('');
}

// ── Results ──
function renderFTResults() {
    const grid = el('ft-results-grid');
    const completedTrades = ftTrades.filter(t => t.type.includes('SELL'));
    const wins = completedTrades.filter(t => t.pnl >= 0).length;
    const losses = completedTrades.length - wins;
    const totalPnl = completedTrades.reduce((s, t) => s + (t.pnl || 0), 0);
    const finalValue = ftCash;
    const returnPct = ((finalValue - ftInitialCash) / ftInitialCash * 100);

    // Buy & hold comparison
    let buyHoldReturn = 0;
    if (ftCandles.length >= 2) {
        const firstPrice = ftCandles[0].Close ?? ftCandles[0].close;
        const lastPrice = ftCandles[ftCandles.length - 1].Close ?? ftCandles[ftCandles.length - 1].close;
        buyHoldReturn = ((lastPrice - firstPrice) / firstPrice * 100);
    }

    const metrics = [
        { label: 'Final Portfolio', value: `₹${finalValue.toLocaleString('en-IN', { maximumFractionDigits: 0 })}`, cls: returnPct >= 0 ? 'positive' : 'negative', sub: `${returnPct >= 0 ? '+' : ''}${returnPct.toFixed(2)}% ROI` },
        { label: 'Total P&L', value: `${totalPnl >= 0 ? '+' : ''}₹${totalPnl.toFixed(0)}`, cls: totalPnl >= 0 ? 'positive' : 'negative', sub: `${completedTrades.length} trades` },
        { label: 'Win Rate', value: completedTrades.length > 0 ? `${(wins/completedTrades.length*100).toFixed(0)}%` : 'N/A', cls: '', sub: `${wins}W / ${losses}L` },
        { label: 'Buy & Hold', value: `${buyHoldReturn >= 0 ? '+' : ''}${buyHoldReturn.toFixed(2)}%`, cls: buyHoldReturn >= 0 ? 'positive' : 'negative', sub: 'Benchmark comparison' },
    ];

    grid.innerHTML = metrics.map(m => `
        <div class="ft-result-card">
            <div class="ft-result-label">${m.label}</div>
            <div class="ft-result-value ${m.cls}">${m.value}</div>
            <div class="ft-result-sub">${m.sub}</div>
        </div>
    `).join('');
}

// ── Utils ──
function fmtTime(c) {
    const ts = c.timestamp || c.Datetime || c.datetime || c.Date || c.date || '';
    if (!ts) return '--:--';
    const d = new Date(ts);
    if (isNaN(d.getTime())) return String(ts).slice(11, 16) || '--:--';
    return d.toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', hour12: false });
}

function resetFTState() {
    ftCandles = []; ftIndex = 0; ftTotalCandles = 0;
    ftCash = 100000; ftShares = 0; ftEntryPrice = 0; ftTrades = [];
    ftSelectedDate = null;
    if (ftChart) { ftChart.destroy(); ftChart = null; }
    setFTStatus('');
}

function setFTStatus(msg) {
    el('ft-status').textContent = msg;
}
