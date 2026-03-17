// ==================== DATA DISPLAY ====================
import { STATE, formatPrice, formatNumber, getRsiColor, getRsiBackground, CONFIG, sanitizeMarkdown } from './config.js';
import { renderChart, destroyExistingCharts } from './charts.js';

export function displayData(data) {
    // Destroy existing charts before rendering new ones
    destroyExistingCharts();
    
    const companyInfo = STATE.stockDatabase.find(s => s.symbol === data.ticker) || { name: 'Technical Analysis Data' };
    document.getElementById('output').innerHTML = `
        <div class="ticker-display">
            <div class="ticker-symbol-main">${data.ticker}</div>
            <div class="ticker-name-main">${companyInfo.name}</div>
        </div>
    `;

    const grid = document.createElement('div');
    grid.className = 'data-grid';
    const cards = [ 
        { id: 'visualization-card', title: 'Technical Charts & Visualizations', icon: '📊', contentHtml: createVisualizationContent(data), isOpen: false },
        { id: 'rule-based-card', title: 'Technical Analysis', icon: '🔍', contentHtml: createAnalysisContent(data.Rule_Based_Analysis), isOpen: false },
        ...(data.AI_Review ? [{ id: 'ai-review-card', title: 'AI Review & Summary', icon: '🤖', contentHtml: createAnalysisContent(data.AI_Review), isOpen: false }] : []),
        { id: 'ohlcv-card', title: 'Raw OHLCV Data', icon: '📈', contentHtml: createOhlcvTable(data.OHLCV), isOpen: false },
        { id: 'ma-rsi-card', title: 'Raw Technical Indicators', icon: '📉', contentHtml: createMaRsiContent(data.MA, data.RSI), isOpen: false },
        { id: 'macd-card', title: 'Raw MACD Data', icon: '🎯', contentHtml: createMacdTable(data.MACD), isOpen: false }
    ];

    cards.forEach(cardData => {
        const card = createDataCard(cardData);
        card.classList.add('full-width');
        card.querySelector('.card-header').addEventListener('click', function(e) {
            if (e.target.closest('.period-selector')) return; // Ignore clicks on the period selector
            const content = card.querySelector('.card-content');
            const icon = card.querySelector('.dropdown-icon');
            const isCollapsed = content.classList.contains('collapsed');
            
            content.classList.toggle('collapsed', !isCollapsed);
            icon.classList.toggle('collapsed', !isCollapsed); 

            if (cardData.id === 'visualization-card') {
                if (isCollapsed) {
                    // If expanding, render the default chart if it doesn't exist
                    if (!STATE.charts.ohlcv) renderChart('ohlcv', data);
                }
            }
        });
        grid.appendChild(card);
    });
    document.getElementById('output').appendChild(grid);

    // Reset chart tabs to default (OHLCV) when new data is loaded
    // First, remove duplicate code - the tabs event listener below handles this
    
    // Add event listeners for chart tabs
    const tabs = document.querySelectorAll('.chart-tab');
    
    // Reset chart tabs to default (OHLCV) when new data is loaded
    tabs.forEach(t => t.classList.remove('active'));
    const ohlcvTab = document.querySelector('.chart-tab[data-chart="ohlcv"]');
    if (ohlcvTab) ohlcvTab.classList.add('active');
    
    document.querySelectorAll('.chart-container').forEach(c => {
        c.classList.remove('active');
        c.style.display = 'none';
    });
    const ohlcvContainer = document.getElementById('chart-ohlcv');
    if (ohlcvContainer) {
        ohlcvContainer.classList.add('active');
        ohlcvContainer.style.display = 'block';
    }
    
    tabs.forEach(tab => {
        tab.addEventListener('click', (e) => {
            const chartType = e.target.dataset.chart;
            
            // Update active tab UI
            tabs.forEach(t => t.classList.remove('active'));
            e.target.classList.add('active');
            
            // Hide all chart containers
            document.querySelectorAll('.chart-container').forEach(c => {
                c.classList.remove('active');
                c.style.display = 'none';
            });
            
            // Show and render selected chart
            const activeContainer = document.getElementById(`chart-${chartType}`);
            if (activeContainer) {
                activeContainer.classList.add('active');
                activeContainer.style.display = 'block';
                if (window.currentChartData) {
                    renderChart(chartType, window.currentChartData);
                }
            }
        });
    });
}

function createDataCard({ id, title, icon, contentHtml, isOpen }) {
    const card = document.createElement('div');
    card.id = id;
    card.className = 'data-card';

    const periodSelectorHtml = (id === 'ohlcv-card' || id === 'ma-rsi-card' || id === 'macd-card') ? `
        <div class="period-selector">
            <label>View:</label>
            <select class="period-select" data-target-card-id="${id}">
                <option value="7" selected>Last 7 Days</option>
                <option value="15">Last 15 Days</option>
                <option value="30">Last 30 Days</option>
            </select>
        </div>
    ` : '';

    card.innerHTML = `
        <div class="card-header">
            <div class="card-title">
                <h2><span>${icon}</span>${title}</h2>
            </div>
            ${periodSelectorHtml}
            <span class="dropdown-toggle" aria-label="Toggle card content"><span class="dropdown-icon ${isOpen ? '' : 'collapsed'}">▼</span></span>
        </div>
        <div class="card-content ${isOpen ? '' : 'collapsed'}">
            ${contentHtml}
        </div>
    `;
    return card;
}

function createVisualizationContent(data) {
    if (!data.OHLCV?.length) {
        return '<div class="unavailable-notice"><strong>⚠️ Data Unavailable</strong><p>Cannot generate charts without OHLCV data.</p></div>';
    }

    const tabsHtml = `
        <div class="chart-tabs">
            <button class="chart-tab active" data-chart="ohlcv">Price & Volume</button>
            <button class="chart-tab" data-chart="movingAverages">Moving Averages</button>
            ${data.RSI?.length ? `<button class="chart-tab" data-chart="rsi">RSI</button>` : ''}
            ${data.MACD?.length ? `<button class="chart-tab" data-chart="macd">MACD</button>` : ''}
        </div>
    `;

    const chartContainers = `
        <div class="chart-container active" id="chart-ohlcv">
            <canvas id="ohlcvChart"></canvas>
        </div>
        <div class="chart-container" id="chart-movingAverages">
            <canvas id="movingAveragesChart"></canvas>
        </div>
        ${data.RSI?.length ? `<div class="chart-container" id="chart-rsi"><canvas id="rsiChart"></canvas></div>` : ''}
        ${data.MACD?.length ? `<div class="chart-container" id="chart-macd"><canvas id="macdChart"></canvas></div>` : ''}
    `;

    return tabsHtml + chartContainers;
}

function createAnalysisContent(content) {
    if (!content) {
        return '<div class="unavailable-notice"><strong>⚠️ Analysis Unavailable</strong><p>Could not retrieve rule-based or AI analysis for this security.</p></div>';
    }
    const html = sanitizeMarkdown(content);
    return `<div class="analysis-content">${html}</div>`;
}

function createOhlcvTable(data) {
    if (!data?.length) {
        return '<div class="unavailable-notice"><strong>⚠️ Data Unavailable</strong><p>No OHLCV data available.</p></div>';
    }

    return `
        <div class="table-scroll-wrapper">
            <div class="data-table">
                <table>
                    <thead>
                        <tr><th>Date</th><th>Open</th><th>High</th><th>Low</th><th>Close</th><th>Volume</th></tr>
                    </thead>
                    <tbody>
                        ${data.map((item, index) => `
                            <tr style="${index < data.length - 7 ? 'display: none;' : ''}">
                                <td>${item.Date || 'N/A'}</td>
                                <td>${formatPrice(item.Open)}</td>
                                <td>${formatPrice(item.High)}</td>
                                <td>${formatPrice(item.Low)}</td>
                                <td>${formatPrice(item.Close)}</td>
                                <td>${formatNumber(item.Volume)}</td>
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
            </div>
        </div>
        <p style="font-size: 0.9rem; color: #6b7280; margin-top: 10px;">Showing latest ${data.length} trading sessions</p>
    `;
}

function createMaRsiContent(maData, rsiData) {
    let content = '';

    if (maData?.length) {
        content += `
            <h4>Moving Averages (Latest)</h4>
            <div class="table-scroll-wrapper">
                <div class="data-table data-table-ma">
                    <table>
                        <thead><tr><th>Date</th><th>MA5</th><th>MA10</th></tr></thead>
                        <tbody>
                            ${maData.map((item, index) => `
                                <tr style="${index < maData.length - 7 ? 'display: none;' : ''}">
                                    <td>${item.Date || 'N/A'}</td>
                                    <td>${formatPrice(item.MA5)}</td>
                                    <td>${formatPrice(item.MA10)}</td>
                                </tr>
                            `).join('')}
                        </tbody>
                    </table>
                </div>
            </div>
        `;
    }

    if (rsiData?.length) {
        content += createRsiTable(rsiData);
    }

    if (!content) {
        return '<div class="unavailable-notice"><strong>⚠️ Data Unavailable</strong><p>No Moving Average or RSI data available.</p></div>';
    }

    return content;
}

function createRsiTable(rsiData) {
    return `
        <h4>Relative Strength Index (RSI) - Latest</h4>
        <div class="table-scroll-wrapper">
            <div class="data-table data-table-rsi">
                <table>
                    <thead><tr><th>Date</th><th>RSI</th><th>Status</th></tr></thead>
                    <tbody>
                        ${rsiData.map((item, index) => {
                            const rsi = item.RSI;
                            const color = getRsiColor(rsi);
                            const background = getRsiBackground(rsi);
                            let status = 'Neutral';
                            if (rsi > 70) status = 'Overbought';
                            if (rsi < 30) status = 'Oversold';

                            return `
                                <tr style="${index < rsiData.length - 7 ? 'display: none;' : ''}">
                                    <td>${item.Date || 'N/A'}</td>
                                    <td style="color: ${color}; font-weight: 700;">${rsi != null ? rsi.toFixed(2) : 'N/A'}</td>
                                    <td style="background: ${background}; color: ${color}; font-weight: 600; border-radius: 6px; padding: 4px 8px; font-size: 0.9em; text-align: center;">${status}</td>
                                </tr>
                            `;
                        }).join('')}
                    </tbody>
                </table>
            </div>
        </div>
        <p style="font-size: 0.85rem; color: #6b7280; margin-top: 10px;">RSI above 70 is considered Overbought, below 30 is Oversold.</p>
    `;
}

function createMacdTable(data) {
    if (!data?.length) {
        return '<div class="unavailable-notice"><strong>⚠️ Data Unavailable</strong><p>No MACD data available.</p></div>';
    }

    return `
        <h4>Moving Average Convergence Divergence (MACD) - Latest</h4>
        <div class="table-scroll-wrapper">
            <div class="data-table data-table-macd">
                <table>
                    <thead><tr><th>Date</th><th>MACD</th><th>Signal</th><th>Histogram</th></tr></thead>
                    <tbody>
                        ${data.map((item, index) => {
                            const histClass = item.Histogram > 0 ? 'positive-hist' : (item.Histogram < 0 ? 'negative-hist' : '');
                            return `
                                <tr style="${index < data.length - 7 ? 'display: none;' : ''}">
                                    <td>${item.Date || 'N/A'}</td>
                                    <td>${item.MACD != null ? item.MACD.toFixed(2) : 'N/A'}</td>
                                    <td>${item.Signal != null ? item.Signal.toFixed(2) : 'N/A'}</td>
                                    <td class="${histClass}">${item.Histogram != null ? item.Histogram.toFixed(2) : 'N/A'}</td>
                                </tr>
                            `;
                        }).join('')}
                    </tbody>
                </table>
            </div>
        </div>
        <p style="font-size: 0.85rem; color: #6b7280; margin-top: 10px;">Histogram is MACD minus Signal line. Positive suggests upward momentum.</p>
    `;
}