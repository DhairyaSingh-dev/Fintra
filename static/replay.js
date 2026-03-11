import { CONFIG } from './config.js';
import { showNotification } from './notifications.js';

// ==================== REPLAY MODULE (REST-based) ====================
let replayChart = null;
let allCandles = [];
let totalCandles = 0;
let currentIndex = 0;
let isPlaying = false;
let playbackSpeed = 1.0;
let playbackTimer = null;
let selectedDate = null;

// ── Public entry point ──
export function openReplayModal() {
    const modal = document.getElementById('replay-modal');
    if (!modal) return;
    modal.showModal();

    const symbol = window.currentBacktestData?.symbol || '';
    const label = document.getElementById('replay-symbol-label');
    if (label) {
        label.textContent = symbol
            ? `Replaying ${symbol}`
            : 'Select a time window to replay';
    }

    showSetup();
    populateDateGrid();
    populateTimeDropdowns();
    bindSetupEvents();
    bindPlayerEvents();
}
window.openReplayModal = openReplayModal;

// ── Setup / Player toggle ──
function showSetup() {
    const setup = document.getElementById('replay-setup');
    const player = document.getElementById('replay-player');
    if (setup) setup.style.display = '';
    if (player) player.style.display = 'none';
    resetPlayerState();
}

function showPlayer() {
    const setup = document.getElementById('replay-setup');
    const player = document.getElementById('replay-player');
    if (setup) setup.style.display = 'none';
    if (player) player.style.display = '';
}

// ── Date Grid ──
function populateDateGrid() {
    const grid = document.getElementById('replay-date-grid');
    if (!grid) return;
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

    const monthNames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    const dayNames = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];

    dates.forEach(d => {
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'replay-date-btn';
        btn.innerHTML = `<span class="rdb-day">${dayNames[d.getDay()]}</span><span class="rdb-num">${d.getDate()}</span><span class="rdb-month">${monthNames[d.getMonth()]}</span>`;
        btn.dataset.date = d.toISOString().split('T')[0];
        btn.addEventListener('click', () => {
            grid.querySelectorAll('.replay-date-btn').forEach(b => b.classList.remove('selected'));
            btn.classList.add('selected');
            selectedDate = btn.dataset.date;
            updateDuration();
        });
        grid.appendChild(btn);
    });
}

// ── Time Dropdowns ──
function populateTimeDropdowns() {
    const hourStart = document.getElementById('replay-hour-start');
    const hourEnd = document.getElementById('replay-hour-end');
    const minStart = document.getElementById('replay-min-start');
    const minEnd = document.getElementById('replay-min-end');

    [hourStart, hourEnd].forEach(sel => {
        if (!sel) return;
        sel.innerHTML = '';
        for (let h = 9; h <= 15; h++) {
            const opt = document.createElement('option');
            opt.value = h;
            opt.textContent = h.toString().padStart(2, '0');
            sel.appendChild(opt);
        }
    });

    [minStart, minEnd].forEach(sel => {
        if (!sel) return;
        sel.innerHTML = '';
        for (let m = 0; m < 60; m++) {
            const opt = document.createElement('option');
            opt.value = m;
            opt.textContent = m.toString().padStart(2, '0');
            sel.appendChild(opt);
        }
    });

    if (hourStart) hourStart.value = '10';
    if (minStart) minStart.value = '0';
    if (hourEnd) hourEnd.value = '10';
    if (minEnd) minEnd.value = '30';

    [hourStart, hourEnd, minStart, minEnd].forEach(el => {
        if (el) el.addEventListener('change', updateDuration);
    });
}

function getSelectedTimes() {
    const hs = parseInt(document.getElementById('replay-hour-start')?.value || '10');
    const ms = parseInt(document.getElementById('replay-min-start')?.value || '0');
    const he = parseInt(document.getElementById('replay-hour-end')?.value || '10');
    const me = parseInt(document.getElementById('replay-min-end')?.value || '30');
    return { hs, ms, he, me };
}

function updateDuration() {
    const badge = document.getElementById('replay-duration-badge');
    const launchBtn = document.getElementById('replay-launch-btn');
    const { hs, ms, he, me } = getSelectedTimes();
    const diff = (he * 60 + me) - (hs * 60 + ms);

    if (diff <= 0) {
        if (badge) { badge.textContent = 'Invalid range'; badge.classList.add('invalid'); }
        if (launchBtn) launchBtn.disabled = true;
        return;
    }
    if (diff > 60) {
        if (badge) { badge.textContent = `${diff} min (max 60)`; badge.classList.add('invalid'); }
        if (launchBtn) launchBtn.disabled = true;
        return;
    }

    if (badge) { badge.textContent = `${diff} min · ${diff} candles`; badge.classList.remove('invalid'); }
    if (launchBtn) launchBtn.disabled = !selectedDate;
}

// ── Setup Events ──
function bindSetupEvents() {
    const launchBtn = document.getElementById('replay-launch-btn');
    const closeBtn = document.getElementById('replay-close-btn');

    if (launchBtn) {
        const nb = launchBtn.cloneNode(true);
        launchBtn.parentNode.replaceChild(nb, launchBtn);
        nb.addEventListener('click', handleLaunch);
    }
    if (closeBtn) {
        const nc = closeBtn.cloneNode(true);
        closeBtn.parentNode.replaceChild(nc, closeBtn);
        nc.addEventListener('click', () => {
            cleanup();
            document.getElementById('replay-modal')?.close();
        });
    }
}

async function handleLaunch() {
    const symbol = window.currentBacktestData?.symbol;
    if (!symbol) { showNotification('Run a backtest first to select a symbol.', 'error'); return; }
    if (!selectedDate) { showNotification('Please select a date.', 'error'); return; }

    const { hs, ms, he, me } = getSelectedTimes();
    const startISO = `${selectedDate}T${String(hs).padStart(2,'0')}:${String(ms).padStart(2,'0')}:00`;
    const endISO = `${selectedDate}T${String(he).padStart(2,'0')}:${String(me).padStart(2,'0')}:00`;

    setStatus('Loading candles...');
    const btn = document.getElementById('replay-launch-btn');
    if (btn) { btn.disabled = true; btn.textContent = 'Loading...'; }

    try {
        const url = `${CONFIG.API_BASE_URL}/replay/candles?symbol=${encodeURIComponent(symbol)}&start=${encodeURIComponent(startISO)}&end=${encodeURIComponent(endISO)}`;
        const res = await fetch(url, { credentials: 'include' });
        const data = await res.json();

        if (!res.ok || data.error) {
            throw new Error(data.error || `Server returned ${res.status}`);
        }

        allCandles = data.candles || [];
        totalCandles = data.total || allCandles.length;

        if (allCandles.length === 0) {
            throw new Error('No candle data returned for this time window.');
        }

        showPlayer();
        initChart(symbol);
        renderUpTo(1);
        setStatus(`Ready — ${allCandles.length} candles. Press ▶ to play.`);
    } catch (err) {
        setStatus('');
        showNotification(err.message || 'Failed to load replay data.', 'error');
    } finally {
        if (btn) { btn.disabled = false; btn.innerHTML = '<span>▶</span> Start Replay'; }
    }
}

// ── Chart ──
function initChart(symbol) {
    const canvas = document.getElementById('replay-chart');
    if (!canvas) return;
    if (replayChart) { replayChart.destroy(); replayChart = null; }

    replayChart = new Chart(canvas.getContext('2d'), {
        type: 'line',
        data: {
            labels: [],
            datasets: [
                { label: `${symbol} Close`, data: [], borderColor: '#00D4C8', backgroundColor: 'rgba(0,212,200,0.08)', borderWidth: 2, fill: true, tension: 0.2, pointRadius: 0, pointHoverRadius: 5, pointHoverBackgroundColor: '#00D4C8' },
                { label: 'High', data: [], borderColor: 'rgba(74,222,128,0.45)', borderWidth: 1, borderDash: [3,3], fill: false, tension: 0.2, pointRadius: 0 },
                { label: 'Low', data: [], borderColor: 'rgba(248,113,113,0.45)', borderWidth: 1, borderDash: [3,3], fill: false, tension: 0.2, pointRadius: 0 }
            ]
        },
        options: {
            responsive: true, maintainAspectRatio: false, animation: { duration: 120 },
            plugins: { legend: { display: false }, tooltip: { mode: 'index', intersect: false, callbacks: { label: ctx => `${ctx.dataset.label}: ₹${ctx.parsed.y?.toFixed(2)}` } } },
            scales: {
                x: { ticks: { color: 'rgba(255,255,255,0.5)', maxTicksLimit: 10, font: { size: 11 } }, grid: { color: 'rgba(255,255,255,0.04)' } },
                y: { position: 'right', ticks: { color: 'rgba(255,255,255,0.5)', font: { size: 11 }, callback: v => '₹' + v.toFixed(0) }, grid: { color: 'rgba(255,255,255,0.06)' } }
            },
            interaction: { mode: 'nearest', axis: 'x', intersect: false }
        }
    });
}

function renderUpTo(idx) {
    if (!replayChart || idx < 1) return;
    const slice = allCandles.slice(0, idx);

    replayChart.data.labels = slice.map(c => fmtTime(c));
    replayChart.data.datasets[0].data = slice.map(c => c.Close ?? c.close);
    replayChart.data.datasets[1].data = slice.map(c => c.High ?? c.high);
    replayChart.data.datasets[2].data = slice.map(c => c.Low ?? c.low);
    replayChart.update('none');

    currentIndex = idx;
    const last = slice[slice.length - 1];
    document.getElementById('rc-open').textContent = '₹' + ((last.Open ?? last.open) || 0).toFixed(2);
    document.getElementById('rc-high').textContent = '₹' + ((last.High ?? last.high) || 0).toFixed(2);
    document.getElementById('rc-low').textContent = '₹' + ((last.Low ?? last.low) || 0).toFixed(2);
    document.getElementById('rc-close').textContent = '₹' + ((last.Close ?? last.close) || 0).toFixed(2);
    document.getElementById('rc-vol').textContent = ((last.Volume ?? last.volume) || 0).toLocaleString();
    document.getElementById('rc-time').textContent = fmtTime(last);

    document.getElementById('replay-candle-counter').textContent = `${idx} / ${totalCandles}`;
    const pct = totalCandles > 0 ? (idx / totalCandles) * 100 : 0;
    const fill = document.getElementById('replay-progress-fill');
    const thumb = document.getElementById('replay-progress-thumb');
    if (fill) fill.style.width = pct + '%';
    if (thumb) thumb.style.left = pct + '%';

    if (slice.length > 0) {
        document.getElementById('replay-time-label').textContent = `${fmtTime(slice[0])} → ${fmtTime(last)}`;
    }
}

function fmtTime(c) {
    const ts = c.timestamp || c.Datetime || c.datetime || c.Date || c.date || '';
    if (!ts) return '--:--';
    const d = new Date(ts);
    if (isNaN(d.getTime())) return String(ts).slice(11, 16) || '--:--';
    return d.toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', hour12: false });
}

// ── Transport Controls ──
function bindPlayerEvents() {
    const playBtn = document.getElementById('replay-btn-play');
    const backBtn = document.getElementById('replay-btn-back');
    const fwdBtn = document.getElementById('replay-btn-forward');
    const resetBtn = document.getElementById('replay-btn-reset');
    const progressBar = document.getElementById('replay-progress-bar');

    if (playBtn) playBtn.onclick = () => isPlaying ? pausePlayback() : startPlayback();
    if (backBtn) backBtn.onclick = () => { pausePlayback(); if (currentIndex > 1) renderUpTo(currentIndex - 1); };
    if (fwdBtn) fwdBtn.onclick = () => { pausePlayback(); if (currentIndex < allCandles.length) renderUpTo(currentIndex + 1); };
    if (resetBtn) resetBtn.onclick = () => { cleanup(); showSetup(); };

    document.querySelectorAll('.speed-pill').forEach(pill => {
        pill.onclick = () => {
            document.querySelectorAll('.speed-pill').forEach(p => p.classList.remove('active'));
            pill.classList.add('active');
            playbackSpeed = parseFloat(pill.dataset.speed);
            if (isPlaying) { clearInterval(playbackTimer); playbackTimer = setInterval(playTick, 1000 / playbackSpeed); }
        };
    });

    if (progressBar) {
        progressBar.onclick = (e) => {
            const rect = progressBar.getBoundingClientRect();
            const pct = (e.clientX - rect.left) / rect.width;
            const target = Math.max(1, Math.min(allCandles.length, Math.round(pct * totalCandles)));
            pausePlayback();
            renderUpTo(target);
        };
    }
}

function startPlayback() {
    if (currentIndex >= allCandles.length) currentIndex = 0;
    isPlaying = true;
    document.getElementById('replay-btn-play').textContent = '⏸';
    playbackTimer = setInterval(playTick, 1000 / playbackSpeed);
    setStatus('Playing...');
}

function pausePlayback() {
    isPlaying = false;
    const pb = document.getElementById('replay-btn-play');
    if (pb) pb.textContent = '▶';
    if (playbackTimer) { clearInterval(playbackTimer); playbackTimer = null; }
    setStatus('Paused');
}

function playTick() {
    if (currentIndex < allCandles.length) renderUpTo(currentIndex + 1);
    else { pausePlayback(); setStatus('Replay complete'); }
}

// ── Cleanup ──
function resetPlayerState() {
    allCandles = []; totalCandles = 0; currentIndex = 0;
    isPlaying = false; playbackSpeed = 1.0;
    if (playbackTimer) { clearInterval(playbackTimer); playbackTimer = null; }
    if (replayChart) { replayChart.destroy(); replayChart = null; }
    document.querySelectorAll('.speed-pill').forEach(p => p.classList.remove('active'));
    document.querySelector('.speed-pill[data-speed="1"]')?.classList.add('active');
    setStatus('');
}

function cleanup() {
    pausePlayback();
    resetPlayerState();
}

function setStatus(msg) {
    const el = document.getElementById('replay-status');
    if (el) el.textContent = msg;
}
