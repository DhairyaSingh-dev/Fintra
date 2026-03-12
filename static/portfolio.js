import { deps, debounce, getRsiColor } from './config.js';
import { showNotification } from './notifications.js';
import { updateChatContextIndicator } from './chat.js';
import { getAuthHeaders, handleLogout } from './auth.js';

const { DOM, CONFIG, STATE } = deps;

let positionToDeleteId = null;

export function initializePortfolio() {
    // Event Listeners for portfolio functionality
    DOM.portfolioTabBtn?.addEventListener('click', showPortfolioView);
    DOM.searchTabBtn?.addEventListener('click', showSearchView);
    DOM.backtestingTabBtn?.addEventListener('click', showBacktestingView);
    DOM.addPositionBtn?.addEventListener('click', () => DOM.addPositionModal.showModal());
    DOM.closeModalBtn?.addEventListener('click', () => DOM.addPositionModal.close());
    DOM.addPositionForm?.addEventListener('submit', handleAddPosition);

    // Close modal if clicked outside the form
    DOM.addPositionModal?.addEventListener('click', (e) => {
        if (e.target.id === 'add-position-modal') {
            DOM.addPositionModal.close();
        }
    });

    // --- Delegated Event Listener for Portfolio Cards (Glassmorphism) -
    DOM.portfolioContent?.addEventListener('click', (e) => {
        const deleteBtn = e.target.closest('.delete-btn');
        const searchBtn = e.target.closest('.search-position-btn');

        if (deleteBtn) {
            e.stopPropagation();
            positionToDeleteId = deleteBtn.dataset.id;
            document.getElementById('delete-modal').showModal();
            return;
        }

        if (searchBtn) {
            e.stopPropagation();
            const card = searchBtn.closest('.glass-position-card');
            const symbol = card?.dataset.symbol;
            if (symbol) {
                DOM.searchTabBtn.click();
                DOM.symbol.value = symbol;
                document.querySelector('.search-form').requestSubmit();
            }
        }
    });

    // Delete Modal Listeners
    const deleteModal = document.getElementById('delete-modal');
    const confirmDeleteBtn = document.getElementById('confirm-delete-btn');
    const cancelDeleteBtn = document.getElementById('cancel-delete-btn');

    cancelDeleteBtn?.addEventListener('click', () => deleteModal.close());
    
    confirmDeleteBtn?.addEventListener('click', () => {
        if (positionToDeleteId) {
            executeDeletePosition(positionToDeleteId);
        }
        deleteModal.close();
    });
    
    deleteModal?.addEventListener('click', (e) => {
        if (e.target === deleteModal) deleteModal.close();
    });

    // Initialize Chat Portfolio Menu
    setupChatPortfolioMenu();

    // --- New: Logic for current price indicator ---
    const debouncedPriceFetch = debounce(async (e) => {
        const symbol = e.target.value.trim().toUpperCase();
        if (!symbol) {
            DOM.currentPriceIndicator.style.display = 'none';
            return;
        }
        try {
            const response = await fetch(`${CONFIG.API_BASE_URL}/price/${symbol}`);
            if (response.ok) {
                const data = await response.json();
                DOM.currentPriceIndicator.textContent = `Live: $${data.price.toFixed(2)}`;
                DOM.currentPriceIndicator.style.display = 'block';
            } else {
                DOM.currentPriceIndicator.style.display = 'none';
            }
        } catch (error) {
            DOM.currentPriceIndicator.style.display = 'none';
        }
    }, 1000); // Increased debounce to 1s to prevent 429 Rate Limit errors
    DOM.addPositionSymbolInput?.addEventListener('input', debouncedPriceFetch);
}

function generateSparkline(data, isPositive, width = 100, height = 40) {
    if (!data || data.length < 2) {
        return `<div class="sparkline-container"></div>`;
    }
    
    const min = Math.min(...data);
    const max = Math.max(...data);
    const range = max - min || 1;
    
    const points = data.map((val, i) => {
        const x = (i / (data.length - 1)) * width;
        const y = height - ((val - min) / range) * (height - 8) - 4;
        return `${x},${y}`;
    });
    
    const pathD = `M ${points.join(' L ')}`;
    const areaPoints = `M 0,${height} L ${points.join(' L ')} L ${width},${height} Z`;
    const trendClass = isPositive ? 'positive' : 'negative';
    
    return `
        <div class="sparkline-container">
            <svg width="${width}" height="${height}" viewBox="0 0 ${width} ${height}">
                <path class="sparkline-area ${trendClass}" d="${areaPoints}" />
                <path class="sparkline ${trendClass}" d="${pathD}" />
            </svg>
        </div>
    `;
}

function generateRandomSparkline(isPositive) {
    const points = 14;
    const data = [];
    let value = 50 + Math.random() * 30;
    
    for (let i = 0; i < points; i++) {
        const change = (Math.random() - (isPositive ? 0.3 : 0.7)) * 15;
        value = Math.max(10, Math.min(90, value + change));
        data.push(value);
    }
    
    return generateSparkline(data, isPositive);
}

function renderPortfolioSummary(positions) {
    const totalValue = positions.reduce((sum, p) => sum + (p.current_value || 0), 0);
    const totalCost = positions.reduce((sum, p) => sum + (p.quantity * p.entry_price), 0);
    const totalPnL = totalValue - totalCost;
    const pnlPercent = totalCost > 0 ? (totalPnL / totalCost) * 100 : 0;
    
    const dayChange = positions.reduce((sum, p) => {
        const change = p.current_price - p.entry_price;
        return sum + (change * p.quantity);
    }, 0);
    const dayChangePercent = totalCost > 0 ? (dayChange / totalCost) * 100 : 0;
    
    const positiveCount = positions.filter(p => p.pnl >= 0).length;
    
    return `
        <div class="portfolio-summary">
            <div class="summary-card">
                <div class="summary-label">Total Value</div>
                <div class="summary-value animate-count">$${totalValue.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</div>
            </div>
            <div class="summary-card">
                <div class="summary-label">Total P&L</div>
                <div class="summary-value ${totalPnL >= 0 ? 'positive' : 'negative'} animate-count">
                    ${totalPnL >= 0 ? '+' : ''}$${totalPnL.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                </div>
                <div class="summary-change ${pnlPercent >= 0 ? 'positive' : 'negative'}">
                    ${pnlPercent >= 0 ? '↑' : '↓'} ${Math.abs(pnlPercent).toFixed(2)}%
                </div>
            </div>
            <div class="summary-card">
                <div class="summary-label">Day Change</div>
                <div class="summary-value ${dayChange >= 0 ? 'positive' : 'negative'} animate-count">
                    ${dayChange >= 0 ? '+' : ''}$${Math.abs(dayChange).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                </div>
                <div class="summary-change ${dayChangePercent >= 0 ? 'positive' : 'negative'}">
                    ${dayChangePercent >= 0 ? '↑' : '↓'} ${Math.abs(dayChangePercent).toFixed(2)}%
                </div>
            </div>
            <div class="summary-card">
                <div class="summary-label">Positions</div>
                <div class="summary-value animate-count">${positions.length}</div>
                <div class="summary-change positive">${positiveCount} ↑ ${positions.length - positiveCount} ↓</div>
            </div>
        </div>
    `;
}

function showSearchView() {
    DOM.searchView.style.display = 'block';
    DOM.portfolioView.style.display = 'none';
    DOM.backtestingView.style.display = 'none';
    DOM.searchTabBtn.classList.add('active');
    DOM.portfolioTabBtn.classList.remove('active');
    DOM.backtestingTabBtn.classList.remove('active');
}

function showPortfolioView() {
    DOM.searchView.style.display = 'none';
    DOM.portfolioView.style.display = 'block';
    DOM.backtestingView.style.display = 'none';
    DOM.searchTabBtn.classList.remove('active');
    DOM.portfolioTabBtn.classList.add('active');
    DOM.backtestingTabBtn.classList.remove('active');
    fetchAndDisplayPortfolio();
}

function showBacktestingView() {
    DOM.searchView.style.display = 'none';
    DOM.portfolioView.style.display = 'none';
    DOM.backtestingView.style.display = 'block';
    DOM.searchTabBtn.classList.remove('active');
    DOM.portfolioTabBtn.classList.remove('active');
    DOM.backtestingTabBtn.classList.add('active');
}

async function fetchAndDisplayPortfolio() {
    const portfolioContent = DOM.portfolioContent;
    portfolioContent.innerHTML = `
        <div class="loading">
            <div class="spinner"></div>
            <div class="loading-text">Loading portfolio...</div>
            <div class="loading-progress-container">
                <div class="loading-progress-bar" id="portfolio-loading-progress"></div>
            </div>
            <div class="loading-phase" id="portfolio-loading-phase">Initializing...</div>
        </div>
    `;

    const updateProgress = (percent, phase) => {
        const progressBar = document.getElementById('portfolio-loading-progress');
        const phaseText = document.getElementById('portfolio-loading-phase');
        if (progressBar) progressBar.style.width = percent + '%';
        if (phaseText) phaseText.textContent = phase;
    };

    try {
        updateProgress(15, 'Connecting to server...');
        
        const response = await fetch(`${CONFIG.API_BASE_URL}/portfolio`, {
            credentials: 'include',
            headers: getAuthHeaders()
        });
        
        if (!response.ok) {
            if (response.status === 401) {
                showNotification('Session expired or invalid. Please sign in again.', 'error');
                handleLogout(false); // Force logout to clear invalid token
                return;
            }
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        updateProgress(40, 'Fetching position data...');
        const positions = await response.json();
        
        updateProgress(70, 'Calculating indicators & P&L...');
        
        // Store in global state for chat context
        STATE.portfolio = positions;

        if (positions.length === 0) {
            portfolioContent.innerHTML = `
                <div class="portfolio-empty">
                    <div class="portfolio-empty-icon">📊</div>
                    <h3>Your portfolio is empty</h3>
                    <p>Click "Add Position" to start tracking your investments.</p>
                </div>
            `;
            return;
        }

        updateProgress(90, 'Rendering portfolio view...');
        
        const summaryHtml = renderPortfolioSummary(positions);
        const cardsHtml = positions.map((pos, i) => createPositionCard(pos, i)).join('');
        portfolioContent.innerHTML = `${summaryHtml}<div class="portfolio-cards">${cardsHtml}</div>`;
        
        updateProgress(100, 'Complete!');

    } catch (error) {
        console.error('❌ Error fetching portfolio:', error);
        portfolioContent.innerHTML = `<div class="error" style="display: block;">Failed to fetch portfolio data. Please try again.</div>`;
    }
}

async function handleAddPosition(e) {
    e.preventDefault();
    const formData = new FormData(e.target);
    const positionData = Object.fromEntries(formData.entries());

    if (parseFloat(positionData.quantity) < 0) {
        showNotification('Quantity cannot be negative.', 'error');
        return;
    }

    try {
        const response = await fetch(`${CONFIG.API_BASE_URL}/positions`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                ...getAuthHeaders()
            },
            credentials: 'include',
            body: JSON.stringify(positionData)
        });

        const result = await response.json();

        if (!response.ok) {
            if (response.status === 401) {
                showNotification('Session expired. Please sign in again.', 'error');
                DOM.addPositionModal.close();
                handleLogout(false);
                return;
            }
            throw new Error(result.error || 'Failed to add position.');
        }

        showNotification('Position added successfully!', 'success');
        DOM.addPositionModal.close();
        e.target.reset();
        fetchAndDisplayPortfolio(); // Refresh the view

    } catch (error) {
        console.error('❌ Error adding position:', error);
        showNotification(error.message, 'error');
    }
}

async function executeDeletePosition(positionId) {
    try {
        const response = await fetch(`${CONFIG.API_BASE_URL}/positions/${positionId}`, {
            method: 'DELETE',
            credentials: 'include',
            headers: getAuthHeaders()
        });

        if (!response.ok) {
            if (response.status === 401) {
                showNotification('Session expired.', 'error');
                handleLogout(false);
                return;
            }
            const result = await response.json();
            throw new Error(result.error || 'Failed to delete position.');
        }

        showNotification('Position deleted successfully.', 'success');
        fetchAndDisplayPortfolio(); // Refresh the view

    } catch (error) {
        console.error('❌ Error deleting position:', error);
        showNotification(error.message, 'error');
    }
}

function setupChatPortfolioMenu() {
    const btn = document.getElementById('chat-portfolio-btn');
    const menu = document.getElementById('chat-portfolio-menu');
    const checkbox = document.getElementById('chat-use-portfolio');
    const contextHeader = document.getElementById('chat-context-header');

    if (!btn || !menu) return;

    btn.addEventListener('click', (e) => {
        e.stopPropagation();
        // Repopulate menu on click to ensure fresh data
        renderChatPortfolioMenu(menu);
        menu.classList.toggle('active');
        btn.classList.toggle('active');
    });

    // Delegated listener for menu items
    menu.addEventListener('click', (e) => {
        e.stopPropagation(); // Prevent menu from closing
        const clearBtn = e.target.closest('[data-action="clear"]');
        const checkbox = e.target.closest('input[type="checkbox"]');

        if (clearBtn) {
            STATE.chatContextSymbols = [];
            updateChatContextIndicator();
            menu.classList.remove('active');
            btn.classList.remove('active');
        } else if (checkbox) {
            // The 'change' event will handle the logic
        }
    });

    menu.addEventListener('change', (e) => {
        if (e.target.type === 'checkbox') {
            const symbol = e.target.dataset.symbol;
            if (e.target.checked) {
                if (!STATE.chatContextSymbols.includes(symbol)) STATE.chatContextSymbols.push(symbol);
            } else {
                STATE.chatContextSymbols = STATE.chatContextSymbols.filter(s => s !== symbol);
            }
            updateChatContextIndicator();
        }
    });

    // Close menu when clicking outside
    document.addEventListener('click', (e) => {
        if (!menu.contains(e.target) && e.target !== btn) {
            menu.classList.remove('active');
            btn.classList.remove('active');
        }
    });
}

function renderChatPortfolioMenu(menu) {
    const positions = STATE.portfolio || [];
    
    let html = `<div class="chat-portfolio-item" data-action="clear"><label>None (Clear Context)</label></div>`;
    
    positions.forEach(pos => {
        const isSelected = STATE.chatContextSymbols.includes(pos.symbol);
        html += `
            <div class="chat-portfolio-item">
                <input type="checkbox" id="chat-ctx-${pos.symbol}" data-symbol="${pos.symbol}" ${isSelected ? 'checked' : ''}>
                <label for="chat-ctx-${pos.symbol}">${pos.symbol}</label>
            </div>
        `;
    });

    menu.innerHTML = html;
}

function renderPositionChart(pos) {
    const canvas = document.getElementById(`chart-${pos.id}`);
    if (!canvas) return;
    const ctx = canvas.getContext('2d');

    const chartData = pos.chart_data;
    const dates = chartData.map(d => d.Date.substring(5));
    const closes = chartData.map(d => d.Close);

    if (STATE.charts[`chart-${pos.id}`]) {
        STATE.charts[`chart-${pos.id}`].destroy();
    }

    STATE.charts[`chart-${pos.id}`] = new Chart(ctx, {
        type: 'line',
        data: {
            labels: dates,
            datasets: [{
                label: 'Close Price',
                data: closes,
                borderColor: '#667eea',
                backgroundColor: 'rgba(102, 126, 234, 0.1)',
                borderWidth: 2,
                tension: 0.1,
                fill: true,
                pointRadius: 0,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                title: { display: false }
            },
            scales: {
                x: { display: false },
                y: { display: false }
            }
        }
    });
}

function createPositionCard(pos, index = 0) {
    const pnlClass = pos.pnl >= 0 ? 'positive' : 'negative';
    const pnlSign = pos.pnl >= 0 ? '+' : '';
    const company = STATE.stockDatabase.find(s => s.symbol === pos.symbol);
    const isPositivePnL = pos.pnl >= 0;
    const sparkline = generateRandomSparkline(isPositivePnL);
    
    const pnlPercent = pos.pnl_percent || 0;
    const barWidth = Math.min(Math.abs(pnlPercent), 100);

    return `
        <div class="glass-position-card" data-id="${pos.id}" data-symbol="${pos.symbol}" style="animation-delay: ${index * 50}ms">
            <div class="card-main">
                <div class="card-left">
                    ${sparkline}
                    <div class="symbol-info">
                        <div class="symbol-name">${pos.symbol}</div>
                        <div class="symbol-company">${company?.name || 'N/A'}</div>
                    </div>
                </div>
                <div class="card-right">
                    <div class="price-info">
                        <div class="current-price">$${pos.current_price?.toFixed(2) || '0.00'}</div>
                        <div class="price-change ${pnlClass}">
                            ${pnlSign}$${Math.abs(pos.pnl).toFixed(2)} (${pnlSign}${pnlPercent.toFixed(2)}%)
                        </div>
                    </div>
                    <div class="card-actions">
                        <button class="search-position-btn" title="Search ${pos.symbol}">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/></svg>
                        </button>
                        <button class="delete-btn" data-id="${pos.id}" title="Delete Position">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 6h18M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></svg>
                        </button>
                    </div>
                </div>
            </div>
            <div class="pnl-bar-container">
                <div class="pnl-bar-label">
                    <span>Cost: $${(pos.quantity * pos.entry_price).toFixed(2)}</span>
                    <span>Value: $${pos.current_value?.toFixed(2) || '0.00'}</span>
                </div>
                <div class="pnl-bar">
                    <div class="pnl-bar-fill ${pnlClass}" style="width: ${barWidth}%"></div>
                    <div class="pnl-bar-glow ${pnlClass}"></div>
                </div>
            </div>
            <div class="card-metrics">
                <div class="metric-item">
                    <div class="metric-label">Shares</div>
                    <div class="metric-value">${pos.quantity}</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">Avg Cost</div>
                    <div class="metric-value">$${pos.entry_price?.toFixed(2) || '0.00'}</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">RSI</div>
                    <div class="metric-value" style="color: ${getRsiColor(pos.rsi)}">${pos.rsi?.toFixed(0) || '--'}</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">MACD</div>
                    <div class="metric-value">${pos.macd_status || '--'}</div>
                </div>
            </div>
        </div>
    `;
}