import { deps, getAuthHeaders } from './config.js';
import { handleLogout } from './auth.js';
import { showNotification } from './notifications.js';
import { handleAutocompleteInput, handleAutocompleteKeydown, hideAutocomplete } from './autocomplete.js';

const { DOM, CONFIG, STATE } = deps;

let currentMode = 'beginner';

export function initializeBacktesting() {
    const { DOM } = deps;

    DOM.backtestingTabBtn?.addEventListener('click', showBacktestingView);

    DOM.beginnerModeBtn?.addEventListener('click', () => setMode('beginner'));
    DOM.advancedModeBtn?.addEventListener('click', () => setMode('advanced'));

    DOM.backtestingForm?.addEventListener('submit', handleBacktestSubmit);
    DOM.clearBacktestingBtn?.addEventListener('click', () => {
        DOM.backtestingSymbol.value = '';
        handleBacktestingInput({ target: DOM.backtestingSymbol });
        hideAutocomplete(DOM.backtestingAutocomplete);
    });

    DOM.backtestingSymbol.addEventListener('input', handleBacktestingInput);
    DOM.backtestingSymbol.addEventListener('keydown', (e) => handleAutocompleteKeydown(e, DOM.backtestingAutocomplete));
    DOM.backtestingSymbol.addEventListener('blur', () => {
        // Fetch date range when user leaves the symbol field
        setTimeout(() => {
            const symbol = DOM.backtestingSymbol.value.trim().toUpperCase();
            if (symbol) fetchDateRange(symbol);
        }, 200);
    });

    document.addEventListener('click', (e) => {
        if (!e.target.closest('.input-wrapper')) {
            hideAutocomplete(DOM.backtestingAutocomplete);
        }
    });

    // Override the selectStock function to also fetch date range
    const originalSelectStock = window.selectStock;
    window.selectStock = function (symbol) {
        if (originalSelectStock) originalSelectStock(symbol);
        // Also update backtest symbol if visible
        if (DOM.backtestingSymbol && document.getElementById('backtesting-view')?.style.display !== 'none') {
            DOM.backtestingSymbol.value = symbol;
            fetchDateRange(symbol);
        }
    };

    const strategySelect = document.getElementById('strategy-select');
    if (strategySelect) {
        strategySelect.addEventListener('change', (e) => {
            document.querySelectorAll('.strategy-specific-params').forEach(el => {
                el.style.display = 'none';
            });
            const selectedStrategy = e.target.value;
            const targetParams = document.getElementById(`params-${selectedStrategy}`);
            if (targetParams && currentMode === 'advanced') {
                targetParams.style.display = 'block';
            }
        });
    }

    setDefaultDateRange();
}

function handleBacktestingInput(e) {
    const hasText = e.target.value.trim().length > 0;
    DOM.clearBacktestingBtn?.classList.toggle('visible', hasText);
    handleAutocompleteInput(e, DOM.backtestingAutocomplete);
}

function setMode(mode) {
    currentMode = mode;

    if (mode === 'beginner') {
        if (DOM.beginnerModeBtn) DOM.beginnerModeBtn.classList.add('active');
        if (DOM.advancedModeBtn) DOM.advancedModeBtn.classList.remove('active');
        if (DOM.advancedParams) DOM.advancedParams.style.display = 'none';
        
        // Hide all strategy-specific params when leaving advanced
        document.querySelectorAll('.strategy-specific-params').forEach(el => {
            el.style.display = 'none';
        });
    } else {
        if (DOM.advancedModeBtn) DOM.advancedModeBtn.classList.add('active');
        if (DOM.beginnerModeBtn) DOM.beginnerModeBtn.classList.remove('active');
        if (DOM.advancedParams) DOM.advancedParams.style.display = 'block';

        // Trigger change event to show relevant dynamic params
        const strategySelect = document.getElementById('strategy-select');
        if (strategySelect) {
            strategySelect.dispatchEvent(new Event('change'));
        }
    }
}

// Global variable to store available date range
let availableDateRange = null;

async function fetchDateRange(symbol) {
    if (!symbol) return;

    try {
        const response = await fetch(`${CONFIG.API_BASE_URL}/stock/${symbol}/date_range`, {
            headers: getAuthHeaders()
        });

        if (response.ok) {
            const data = await response.json();
            availableDateRange = data;
            updateDateInputs(data);
            showDateRangeInfo(data);
        }
    } catch (error) {
        console.error('Error fetching date range:', error);
    }
}

function updateDateInputs(dateRange) {
    if (!dateRange) return;

    // Set start date to first available date
    if (DOM.startDate) {
        DOM.startDate.min = dateRange.first_date;
        DOM.startDate.max = dateRange.last_date;
        DOM.startDate.value = dateRange.first_date;
    }

    // Set end date to last available date (already has 31-day lag applied in backend)
    if (DOM.endDate) {
        DOM.endDate.min = dateRange.first_date;
        DOM.endDate.max = dateRange.last_date;
        DOM.endDate.value = dateRange.last_date;
    }
}

function showDateRangeInfo(dateRange) {
    // Remove existing info
    let existingInfo = document.getElementById('date-range-info');
    if (existingInfo) existingInfo.remove();

    // Add date range info banner
    const form = DOM.backtestingForm;
    if (!form) return;

    const infoDiv = document.createElement('div');
    infoDiv.id = 'date-range-info';
    infoDiv.className = 'date-range-info-banner';
    infoDiv.innerHTML = `
        <div class="date-range-content">
            <span class="info-icon">📅</span>
            <div class="info-text">
                <strong>Available Data Range</strong>
                <span>${dateRange.first_date} to ${dateRange.last_date}</span>
                <span class="lag-notice">31-day SEBI compliance lag enforced</span>
            </div>
        </div>
    `;

    form.insertBefore(infoDiv, form.firstChild);
}

function setDefaultDateRange() {
    // Don't set defaults - wait for symbol selection to fetch actual range
    if (DOM.startDate) DOM.startDate.value = '';
    if (DOM.endDate) DOM.endDate.value = '';
}

async function fetchAIAnalysis(backtestConfig, results) {
    try {
        const payload = {
            symbol: backtestConfig.symbol,
            strategy: backtestConfig.strategy,
            start_date: backtestConfig.start_date,
            end_date: backtestConfig.end_date,
            initial_balance: backtestConfig.initial_balance,
            final_portfolio_value: results.final_portfolio_value,
            strategy_return_pct: results.strategy_return_pct,
            market_return_pct: results.market_return_pct,
            sharpe_ratio: results.sharpe_ratio,
            max_drawdown_pct: results.max_drawdown_pct,
            trades: results.trades || []
        };

        const response = await fetch(`${CONFIG.API_BASE_URL}/backtest/ai-analysis`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                ...getAuthHeaders()
            },
            credentials: 'include',
            body: JSON.stringify(payload)
        });

        if (!response.ok) {
            console.warn('AI analysis request failed:', response.status);
            updateAIAnalysisSection("AI analysis unavailable. Server returned an error.");
            return;
        }

        const data = await response.json();
        const aiText = data.ai_analysis || "No AI analysis returned.";
        updateAIAnalysisSection(aiText);

    } catch (error) {
        console.error('AI analysis fetch error:', error);
        updateAIAnalysisSection("AI analysis temporarily unavailable. Please try again later.");
    }
}

function updateAIAnalysisSection(markdownText) {
    const aiContainer = document.querySelector('.backtest-ai-summary .ai-content');
    if (aiContainer) {
        // Remove the loading state
        aiContainer.classList.remove('ai-loading');
        if (typeof marked !== 'undefined') {
            aiContainer.innerHTML = marked.parse(markdownText);
        } else {
            aiContainer.textContent = markdownText;
        }
    }
}

let pyodideBacktestPromise = null;

async function initBacktestPyodide() {
    if (!pyodideBacktestPromise) {
        pyodideBacktestPromise = (async () => {
            showNotification('Initializing Pyodide Backtest Engine...', 'info');
            // 'loadPyodide' is globally available from the script tag in dashboard.html
            const pyodide = await loadPyodide();
            showNotification('Installing Pandas to WebAssembly (~15MB)...', 'info');
            await pyodide.loadPackage("pandas");

            showNotification('Loading Fintra Backtest Engine...', 'info');
            const response = await fetch('/py_backtest_engine.py');
            const pythonCode = await response.text();

            // Execute the python script
            await pyodide.runPythonAsync(pythonCode);
            showNotification('Backtest Engine Ready!', 'success');
            return pyodide;
        })();
    }
    return pyodideBacktestPromise;
}

async function handleBacktestSubmit(e) {
    e.preventDefault();

    const symbol = DOM.backtestingSymbol.value.trim().toUpperCase();
    if (!symbol) {
        showNotification('Please enter a stock symbol', 'error');
        return;
    }

    const formData = new FormData(DOM.backtestingForm);
    const params = Object.fromEntries(formData.entries());

    // Use available date range if no dates selected or invalid dates
    let startDate = params.start_date;
    let endDate = params.end_date;

    if (!startDate && availableDateRange) {
        startDate = availableDateRange.first_date;
    }
    if (!endDate && availableDateRange) {
        endDate = availableDateRange.last_date;
    }

    const backtestConfig = {
        symbol: symbol,
        strategy: params.strategy,
        initial_balance: parseFloat(params.initial_balance) || 100000,
        start_date: startDate,
        end_date: endDate,
        mode: currentMode,
        atr_multiplier: currentMode === 'advanced' ? parseFloat(params.atr_multiplier) || 3.0 : 3.0,
        risk_per_trade: currentMode === 'advanced' ? (parseFloat(params.risk_per_trade) / 100 || 0.02) : 0.02
    };

    showLoading();
    hideError();
    hideResults();
    resetPreviousResults();

    try {
        // Fetch raw stock history data
        const dataResponse = await fetch(`${CONFIG.API_BASE_URL}/stock/${symbol}/history`, {
            headers: getAuthHeaders()
        });

        if (!dataResponse.ok) {
            if (dataResponse.status === 401) {
                showNotification('Session expired. Please sign in again.', 'error');
                handleLogout(false);
                return;
            }
            const err = await dataResponse.json();
            throw new Error(err.error || "Failed to fetch stock history data.");
        }

        const dataJson = await dataResponse.json();

        // Ensure data is available
        if (!dataJson.data || dataJson.data.length === 0) {
            throw new Error('No historical data available for backtesting.');
        }

        // Initialize Python WASM runtime
        const pyodide = await initBacktestPyodide();
        const pyConfig = pyodide.toPy(backtestConfig);

        // We pass the data JSON array string to avoid huge JS-to-Py bridging
        const historicalDataStr = JSON.stringify(dataJson.data);

        // Execute Python backtest natively in JS!
        const resultJsonString = pyodide.globals.get('run_backtest_browser')(historicalDataStr, pyConfig);
        const results = JSON.parse(resultJsonString);
        pyConfig.destroy(); // Free memory

        // Catch ALL error shapes: {error: true, message: "..."} or {error: "some string"}
        if (results.error) {
            const errMsg = typeof results.error === 'string' ? results.error : results.message;
            throw new Error(errMsg || "Failed to run backtest in WASM.");
        }

        // Validate that essential result keys actually exist before rendering
        const requiredKeys = ['final_portfolio_value', 'market_buy_hold_value', 'strategy_return_pct', 'market_return_pct', 'sharpe_ratio', 'max_drawdown_pct'];
        const missingKeys = requiredKeys.filter(k => results[k] === undefined || results[k] === null);
        if (missingKeys.length > 0) {
            console.error('Missing result keys:', missingKeys, 'Full results:', results);
            throw new Error(`Backtest produced incomplete results (missing: ${missingKeys.join(', ')}). Please try a different date range or symbol.`);
        }

        // Show initial results immediately with a spinner (AI analysis loads async)
        results.ai_analysis = '__AI_SPINNER__';

        // Store results for Monte Carlo
        window.currentBacktestData = {
            ...backtestConfig,
            trades: results.trades || [],
            prices: dataJson.data || [],
            strategy_return_pct: results.strategy_return_pct ?? 0,
            sharpe_ratio: results.sharpe_ratio ?? 0,
            max_drawdown_pct: results.max_drawdown_pct ?? 0,
            final_portfolio_value: results.final_portfolio_value ?? 0
        };

        displayBacktestResults(results, backtestConfig);
        showNotification('Client-Side Backtest completed successfully!', 'success');

        // Fire async AI analysis request (non-blocking)
        fetchAIAnalysis(backtestConfig, results);

        // Show Monte Carlo section
        const mcSection = document.getElementById('monte-carlo-section');
        if (mcSection) {
            mcSection.classList.remove('hidden');
        }

        // Show post-backtest options
        const optionsDiv = document.getElementById('post-backtest-options');
        if (optionsDiv) {
            optionsDiv.classList.remove('hidden');
        }

        // Hook option buttons
        const mcBtn = document.getElementById('opt-mc-btn');
        if (mcBtn) {
            // Cleanup existing listeners if any
            const newBtn = mcBtn.cloneNode(true);
            mcBtn.parentNode.replaceChild(newBtn, mcBtn);
            newBtn.addEventListener('click', () => {
                mcSection?.classList.remove('hidden');
                mcSection?.scrollIntoView({ behavior: 'smooth' });
            });
        }

        const replayBtn = document.getElementById('opt-replay-btn');
        if (replayBtn) {
            const newReplay = replayBtn.cloneNode(true);
            replayBtn.parentNode.replaceChild(newReplay, replayBtn);
            newReplay.addEventListener('click', () => {
                if (window.openReplayModal) {
                    window.openReplayModal();
                }
            });
        }

        const forwardBtn = document.getElementById('opt-forward-btn');
        if (forwardBtn) {
            const newFwd = forwardBtn.cloneNode(true);
            forwardBtn.parentNode.replaceChild(newFwd, forwardBtn);
            newFwd.addEventListener('click', () => {
                showNotification('Forward testing not implemented yet.', 'info');
            });
        }

        // Initialize Monte Carlo buttons
        import('./monte_carlo.js').then(mc => {
            if (typeof mc.initializeMonteCarlo === 'function') {
                mc.initializeMonteCarlo();
            }
        }).catch(err => {
            console.error('Failed to load Monte Carlo module:', err);
        });

    } catch (error) {
        console.error('❌ Backtest error:', error);
        showError(error.message);
        showNotification('Backtest failed. Please try again.', 'error');
    } finally {
        hideLoading();
    }
}

function displayBacktestResults(results, params) {
    const container = DOM.backtestingResults;
    if (!container) return;

    container.style.display = 'block';

    const strategyReturnPct = results.strategy_return_pct ?? 0;
    const marketReturnPct = results.market_return_pct ?? 0;
    const roiClass = strategyReturnPct >= 0 ? 'positive' : 'negative';
    const marketRoiClass = marketReturnPct >= 0 ? 'positive' : 'negative';
    const roiSign = strategyReturnPct >= 0 ? '+' : '';
    const marketRoiSign = marketReturnPct >= 0 ? '+' : '';

    const trades = results.trades || [];
    const winningTrades = trades.filter(t => t.result === 'Win').length;
    const losingTrades = trades.filter(t => t.result === 'Loss').length;
    const winRate = trades.length > 0 ? (winningTrades / trades.length * 100).toFixed(1) : 0;

    // Read stat visibility from checkboxes (default all visible in beginner mode)
    const isAdvanced = currentMode === 'advanced';
    const statVisible = {
        finalValue:  !isAdvanced || (document.getElementById('stat-final-value')?.checked !== false),
        buyHold:     !isAdvanced || (document.getElementById('stat-buy-hold')?.checked !== false),
        roi:         !isAdvanced || (document.getElementById('stat-roi')?.checked !== false),
        sharpe:      !isAdvanced || (document.getElementById('stat-sharpe')?.checked !== false),
        maxDrawdown: !isAdvanced || (document.getElementById('stat-max-dd')?.checked !== false),
        winRate:     !isAdvanced || (document.getElementById('stat-win-rate')?.checked !== false),
        tradeCount:  !isAdvanced || (document.getElementById('stat-trade-count')?.checked !== false),
        aiAnalysis:  !isAdvanced || (document.getElementById('stat-ai-analysis')?.checked !== false),
    };

    // Build AI summary
    let aiSummary = '';
    if (results.ai_analysis && statVisible.aiAnalysis) {
        if (results.ai_analysis === '__AI_SPINNER__') {
            aiSummary = `
                <div class="backtest-ai-summary">
                    <h3>🤖 AI Strategy Analysis</h3>
                    <div class="ai-content ai-loading">
                        <div class="ai-spinner"></div>
                        <div class="ai-loading-text">Generating institutional-grade analysis via Groq...</div>
                    </div>
                </div>
            `;
        } else {
            const aiHtml = marked.parse(results.ai_analysis);
            aiSummary = `
                <div class="backtest-ai-summary">
                    <h3>🤖 AI Strategy Analysis</h3>
                    <div class="ai-content">${aiHtml}</div>
                </div>
            `;
        }
    }

    // Build trades section
    let tradesSection = '';
    if (trades.length > 0) {
        // Build the summary stat pills conditionally
        let summaryStats = '';
        if (statVisible.tradeCount) {
            summaryStats += `<div class="trade-stat"><span>Total Trades</span><strong>${trades.length}</strong></div>`;
        }
        if (statVisible.winRate) {
            summaryStats += `
                <div class="trade-stat"><span>Winning</span><strong class="positive">${winningTrades}</strong></div>
                <div class="trade-stat"><span>Losing</span><strong class="negative">${losingTrades}</strong></div>
                <div class="trade-stat"><span>Win Rate</span><strong>${winRate}%</strong></div>
            `;
        }

        tradesSection = `
            <div class="trades-section">
                <h3 class="trades-toggle" onclick="this.nextElementSibling.classList.toggle('expanded'); this.classList.toggle('expanded')">
                    📋 Trade History <span class="toggle-icon">▼</span>
                </h3>
                <div class="trades-content collapsed">
                ${summaryStats ? `<div class="trades-summary">${summaryStats}</div>` : ''}
                <div class="trades-table-wrapper">
                    <table class="trades-table">
                        <thead>
                            <tr>
                                <th>Entry Date</th>
                                <th>Exit Date</th>
                                <th>Entry Price</th>
                                <th>Exit Price</th>
                                <th>P&L %</th>
                                <th>Result</th>
                                <th>Reason</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${trades.map(trade => `
                                <tr class="${trade.result === 'Win' ? 'win-row' : 'loss-row'}">
                                    <td>${trade.entry_date}</td>
                                    <td>${trade.exit_date}</td>
                                    <td>₹${trade.entry_price.toFixed(2)}</td>
                                    <td>₹${trade.exit_price.toFixed(2)}</td>
                                    <td class="${trade.pnl_pct >= 0 ? 'positive' : 'negative'}">
                                        ${trade.pnl_pct >= 0 ? '+' : ''}${trade.pnl_pct.toFixed(2)}%
                                    </td>
                                    <td class="${trade.result === 'Win' ? 'positive' : 'negative'}">${trade.result}</td>
                                    <td>${trade.reason}</td>
                                </tr>
                            `).join('')}
                        </tbody>
                    </table>
                </div>
                </div>
            </div>
        `;
    }

    // Build metric cards conditionally
    let metricCards = '';

    if (statVisible.finalValue) {
        const finalVal = results.final_portfolio_value ?? 0;
        metricCards += `
            <div class="metric-card primary">
                <div class="metric-label">Strategy Final Value</div>
                <div class="metric-value">₹${Number(finalVal).toLocaleString('en-IN', { maximumFractionDigits: 2 })}</div>
                ${statVisible.roi ? `<div class="metric-change ${roiClass}">${roiSign}${strategyReturnPct.toFixed(2)}% ROI</div>` : ''}
            </div>
        `;
    }

    if (statVisible.buyHold) {
        const buyHoldVal = results.market_buy_hold_value ?? 0;
        metricCards += `
            <div class="metric-card secondary">
                <div class="metric-label">Buy & Hold Value</div>
                <div class="metric-value">₹${Number(buyHoldVal).toLocaleString('en-IN', { maximumFractionDigits: 2 })}</div>
                ${statVisible.roi ? `<div class="metric-change ${marketRoiClass}">${marketRoiSign}${marketReturnPct.toFixed(2)}% ROI</div>` : ''}
            </div>
        `;
    }

    if (statVisible.sharpe) {
        const sharpeVal = results.sharpe_ratio ?? 0;
        metricCards += `
            <div class="metric-card">
                <div class="metric-label">Sharpe Ratio</div>
                <div class="metric-value">${Number(sharpeVal).toFixed(2)}</div>
                <div class="metric-sub">Risk-adjusted return</div>
            </div>
        `;
    }

    if (statVisible.maxDrawdown) {
        const maxDdVal = results.max_drawdown_pct ?? 0;
        metricCards += `
            <div class="metric-card">
                <div class="metric-label">Max Drawdown</div>
                <div class="metric-value negative">${Number(maxDdVal).toFixed(2)}%</div>
                <div class="metric-sub">Worst drop from peak</div>
            </div>
        `;
    }

    if (statVisible.winRate && trades.length > 0) {
        metricCards += `
            <div class="metric-card">
                <div class="metric-label">Win Rate</div>
                <div class="metric-value">${winRate}%</div>
                <div class="metric-sub">${winningTrades}W / ${losingTrades}L</div>
            </div>
        `;
    }

    if (statVisible.tradeCount && trades.length > 0) {
        metricCards += `
            <div class="metric-card">
                <div class="metric-label">Total Trades</div>
                <div class="metric-value">${trades.length}</div>
                <div class="metric-sub">Executed positions</div>
            </div>
        `;
    }

    container.innerHTML = `
        <div class="backtest-results-container">
            <div class="backtest-summary">
                <h2>📊 Backtest Results for ${params.symbol}</h2>
                <p class="backtest-period">
                    ${params.start_date} to ${params.end_date} | 
                    Strategy: ${params.strategy.replace('_', ' ').toUpperCase()}
                </p>
                
                <div class="metrics-grid">
                    ${metricCards}
                </div>
            </div>
            
${aiSummary}
    ${tradesSection}
    </div>
    `;
}

let loadingInterval = null;

function showLoading() {
    if (DOM.backtestingLoading) {
        DOM.backtestingLoading.style.display = 'flex';

        // Dynamic loading messages
        const messages = [
            "🔄 Initializing backtest engine...",
            "📊 Loading historical market data...",
            "🧮 Calculating technical indicators...",
            "💡 Analyzing entry and exit signals...",
            "📈 Computing risk metrics...",
            "🤖 Generating AI strategy analysis...",
            "✨ Almost there, finalizing results..."
        ];

        let messageIndex = 0;
        const loadingText = DOM.backtestingLoading.querySelector('.loading-text');

        if (loadingText) {
            loadingText.textContent = messages[0];

            // Cycle through messages every 3 seconds
            loadingInterval = setInterval(() => {
                messageIndex = (messageIndex + 1) % messages.length;
                loadingText.textContent = messages[messageIndex];
            }, 3000);
        }
    }
}

function hideLoading() {
    if (DOM.backtestingLoading) {
        DOM.backtestingLoading.style.display = 'none';
    }

    // Clear the interval
    if (loadingInterval) {
        clearInterval(loadingInterval);
        loadingInterval = null;
    }
}

function showError(message) {
    if (DOM.backtestingError) {
        DOM.backtestingError.innerHTML = `<strong>Error:</strong> ${message}`;
        DOM.backtestingError.style.display = 'block';
    }
}

function hideError() {
    if (DOM.backtestingError) {
        DOM.backtestingError.style.display = 'none';
    }
}

function hideResults() {
    if (DOM.backtestingResults) {
        DOM.backtestingResults.style.display = 'none';
    }
}

function resetPreviousResults() {
    // Clear stored backtest data
    window.currentBacktestData = null;

    // Hide and clear Monte Carlo section
    const mcSection = document.getElementById('monte-carlo-section');
    if (mcSection) {
        mcSection.classList.add('hidden');
    }
    const mcResults = document.getElementById('mc-results');
    if (mcResults) {
        mcResults.innerHTML = '';
    }
    const mcError = document.getElementById('mc-error');
    if (mcError) {
        mcError.textContent = '';
    }

    // Hide post-backtest options
    const optionsDiv = document.getElementById('post-backtest-options');
    if (optionsDiv) {
        optionsDiv.classList.add('hidden');
    }
}

function showBacktestingView() {
    DOM.searchView.style.display = 'none';
    DOM.portfolioView.style.display = 'none';
    DOM.backtestingView.style.display = 'block';

    DOM.searchTabBtn?.classList.remove('active');
    DOM.portfolioTabBtn?.classList.remove('active');
    DOM.backtestingTabBtn?.classList.add('active');
}