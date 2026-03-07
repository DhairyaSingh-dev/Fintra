import { CONFIG } from './config.js';
import { getAuthHeaders } from './auth.js';
import { showNotification } from './notifications.js';

let socket = null;

export function openReplayModal() {
    const modal = document.getElementById('replay-modal');
    if (!modal) return;
    modal.showModal();

    const startInput = document.getElementById('replay-start');
    const endInput = document.getElementById('replay-end');
    const speedSelect = document.getElementById('replay-speed');
    const statusDiv = document.getElementById('replay-status');
    const startBtn = document.getElementById('replay-start-btn');
    const cancelBtn = document.getElementById('replay-cancel-btn');

    // Reset fields
    startInput.value = '';
    endInput.value = '';
    speedSelect.value = '1';
    statusDiv.textContent = '';

    cancelBtn.onclick = () => {
        if (socket) {
            socket.disconnect();
            socket = null;
        }
        modal.close();
    };

    startBtn.onclick = () => {
        const symbol = window.currentBacktestData?.symbol || window.currentBacktestData?.params?.symbol || '';
        const start = startInput.value;
        const end = endInput.value;
        const speed = parseFloat(speedSelect.value);
        if (!symbol || !start || !end) {
            showNotification('Please fill all fields.', 'error');
            return;
        }
        // Validate client‑side: max 60 min, end at least 30 days ago
        const startDt = new Date(start);
        const endDt = new Date(end);
        const diffMs = endDt - startDt;
        if (diffMs <= 0 || diffMs > 60 * 60 * 1000) {
            showNotification('Select a window up to 60 minutes.', 'error');
            return;
        }
        const thirtyDaysAgo = new Date();
        thirtyDaysAgo.setUTCDate(thirtyDaysAgo.getUTCDate() - 30);
        if (endDt > thirtyDaysAgo) {
            showNotification('End time must be at least 30 days in the past (SEBI lag).', 'error');
            return;
        }
        // Establish socket
        if (socket) socket.disconnect();
        socket = io(`${CONFIG.API_BASE_URL.replace('http', 'ws')}/replay`, { transports: ['websocket'] });
        // Init
        socket.emit('init', { symbol, start, end, mode: 'replay' });
        socket.on('ready', (data) => {
            statusDiv.textContent = `Ready – ${data.total} candles`;
            // start streaming
            socket.emit('start');
        });
        socket.on('candle', (candle) => {
            // Append to chart – reuse existing Chart.js instance if any
            const chart = window.liveReplayChart;
            if (chart) {
                chart.data.labels.push(new Date(candle.timestamp));
                chart.data.datasets[0].data.push(candle.close);
                chart.update();
            } else {
                // First candle – create simple line chart in modal
                createReplayChart(candle);
            }
        });
        socket.on('end', () => {
            statusDiv.textContent = 'Replay finished.';
        });
        socket.on('error', (err) => {
            showNotification(err.msg || 'Replay error', 'error');
        });
    };
}

function createReplayChart(firstCandle) {
    // Simple line chart showing close price over time
    const canvas = document.createElement('canvas');
    canvas.id = 'replay-chart';
    canvas.style.width = '100%';
    canvas.style.height = '300px';
    const modal = document.getElementById('replay-modal');
    modal.appendChild(canvas);
    const ctx = canvas.getContext('2d');
    window.liveReplayChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: [new Date(firstCandle.timestamp)],
            datasets: [{
                label: `${firstCandle.symbol || ''} Close`,
                data: [firstCandle.close],
                borderColor: '#3b82f6',
                fill: false,
                tension: 0.1
            }]
        },
        options: {
            scales: {
                x: { type: 'time', time: { unit: 'minute' } },
                y: { beginAtZero: false }
            }
        }
    });
}

// Export for global access used by backtesting.js
window.openReplayModal = openReplayModal;
