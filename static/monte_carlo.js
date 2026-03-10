/**
 * Simplified Monte Carlo Front-end Controller
 */

import { CONFIG } from './config.js';
import { getAuthHeaders } from './auth.js';
import { showNotification } from './notifications.js';

// Monte Carlo State
let mcResults = null;
let isRunning = false;

/**
 * Initialize Monte Carlo functionality
 */
export function initializeMonteCarlo() {
    const quickBtn = document.getElementById('mc-quick-btn');
    const fullBtn = document.getElementById('mc-full-btn');

    if (quickBtn) {
        quickBtn.addEventListener('click', () => runMonteCarloAnalysis(1000));
    }

    if (fullBtn) {
        fullBtn.addEventListener('click', () => runMonteCarloAnalysis(10000));
    }
}

let pyodidePromise = null;

async function initPyodide() {
    if (!pyodidePromise) {
        pyodidePromise = (async () => {
            showNotification('Initializing Pyodide Quant Engine (~10MB)...', 'info');
            // 'loadPyodide' is globally available from the script tag in dashboard.html
            const pyodide = await loadPyodide();
            showNotification('Installing Numpy to WebAssembly...', 'info');
            await pyodide.loadPackage("numpy");

            showNotification('Loading Fintra Quant Models...', 'info');
            const response = await fetch('/py_quant_engine.py');
            const pythonCode = await response.text();

            // Execute the python script to load the functions in the global pyodide scope
            await pyodide.runPythonAsync(pythonCode);
            showNotification('Quant Engine Ready!', 'success');
            return pyodide;
        })();
    }
    return pyodidePromise;
}

/**
 * Run Monte Carlo analysis purely on client via WebAssembly
 */
async function runMonteCarloAnalysis(numSimulations) {
    if (isRunning) {
        showNotification('Analysis in progress...', 'warning');
        return;
    }

    // Get backtest data from global storage
    const backtestData = window.currentBacktestData;

    if (!backtestData) {
        showNotification('Please run a backtest first', 'error');
        return;
    }

    isRunning = true;

    // Disable buttons during analysis
    const quickBtn = document.getElementById('mc-quick-btn');
    const fullBtn = document.getElementById('mc-full-btn');
    if (quickBtn) quickBtn.disabled = true;
    if (fullBtn) fullBtn.disabled = true;

    // Show loading
    const loadingEl = document.getElementById('mc-loading');
    const errorEl = document.getElementById('mc-error');
    if (loadingEl) loadingEl.classList.remove('hidden');
    if (errorEl) errorEl.textContent = '';

    try {
        // Initialize Python WASM runtime
        const pyodide = await initPyodide();

        // Gather stochastic parameters from UI
        const config = {
            num_simulations: numSimulations,
            initial_capital: backtestData.initial_balance || 100000,

            mu: parseFloat(document.getElementById('mc-mu')?.value || 0.05),
            vol: parseFloat(document.getElementById('mc-vol')?.value || 0.20),

            use_heston: document.getElementById('mc-use-heston')?.checked || false,
            kappa: parseFloat(document.getElementById('mc-kappa')?.value || 2.0),
            theta: parseFloat(document.getElementById('mc-theta')?.value || 0.04),
            xi: parseFloat(document.getElementById('mc-xi')?.value || 0.1),
            rho: parseFloat(document.getElementById('mc-rho')?.value || -0.5),

            use_jumps: document.getElementById('mc-use-jumps')?.checked || false,
            lambda_j: parseFloat(document.getElementById('mc-lambda')?.value || 0.5),
            mu_j: parseFloat(document.getElementById('mc-mu-j')?.value || -0.05),
            sigma_j: parseFloat(document.getElementById('mc-sigma-j')?.value || 0.1)
        };

        // Execute Python function natively in JS!
        // Convert JS Object to Python Dictionary implicitly
        const pyConfig = pyodide.toPy(config);
        const resultJsonString = pyodide.globals.get('run_advanced_simulation')(pyConfig);

        // Parse the returned JSON back to JS
        mcResults = JSON.parse(resultJsonString);
        pyConfig.destroy(); // Free memory

        if (mcResults.error) {
            throw new Error("Quant Engine Error: " + mcResults.error);
        }

        // Render results
        renderMCResults(mcResults);

        // Show results section
        const mcSection = document.getElementById('monte-carlo-section');
        if (mcSection) mcSection.classList.remove('hidden');

        showNotification('Institutional simulation complete!', 'success');

    } catch (error) {
        console.error('Monte Carlo WASM error:', error);
        if (errorEl) errorEl.textContent = error.message;
        showNotification('Quant simulation failed', 'error');
    } finally {
        isRunning = false;
        if (loadingEl) loadingEl.classList.add('hidden');

        // Re-enable buttons
        if (quickBtn) quickBtn.disabled = false;
        if (fullBtn) fullBtn.disabled = false;
    }
}

/**
 * Render Monte Carlo results
 */
function renderMCResults(data) {
    const resultsEl = document.getElementById('mc-results');
    if (!resultsEl || !data) return;

    const stats = data.statistics || {};
    const percentiles = data.percentiles || {};
    const meta = data.metadata || {};

    resultsEl.innerHTML = `
        <div class="mc-header-bar">
            <div class="mc-meta">
                <span>WASM Execution</span>
                <span>${meta.num_simulations || 0} Paths</span>
                <span>Time Horizon: ${meta.steps || 252} Days</span>
            </div>
        </div>
        
        <div class="mc-metrics">
            <div class="mc-metric-card highlight">
                <label>Median Return</label>
                <value class="${percentiles.p50 >= 0 ? 'positive' : 'negative'}">${(percentiles.p50 || 0).toFixed(2)}%</value>
                <small>50th Percentile</small>
            </div>
            
            <div class="mc-metric-card">
                <label>Mean Return</label>
                <value class="${stats.mean_return >= 0 ? 'positive' : 'negative'}">${(stats.mean_return || 0).toFixed(2)}%</value>
            </div>
            
            <div class="mc-metric-card">
                <label>Expected Shortfall (CVaR)</label>
                <value class="negative">${(stats.cvar_95 || 0).toFixed(2)}%</value>
            </div>
            
            <div class="mc-metric-card">
                <label>95% Value at Risk (VaR)</label>
                <value class="negative">${(stats.var_95 || 0).toFixed(2)}%</value>
            </div>
            
            <div class="mc-metric-card">
                <label>95th Percentile (Best)</label>
                <value class="positive">${(percentiles.p95 || 0).toFixed(2)}%</value>
            </div>
            
            <div class="mc-metric-card">
                <label>Prob of Ruin (>50% Loss)</label>
                <value class="${stats.prob_ruin > 5 ? 'negative' : ''}">${(stats.prob_ruin || 0).toFixed(2)}%</value>
            </div>
        </div>
        
        <div class="mc-analysis-text">
            <h4>Quantitative Risk Assessment</h4>
            <p>Calculated using <strong>Geometric Brownian Motion</strong> with optional Heston Stochastic Volatility and Merton Jump-Diffusion models.</p>
            <p>The median projected outcome is <strong>${(percentiles.p50 || 0).toFixed(2)}%</strong>. In the worst 5% of systemic shock scenarios, the average portfolio loss (CVaR) drops to <strong>${(stats.cvar_95 || 0).toFixed(2)}%</strong>.</p>
            <p>Mean Maximum Drawdown probability across all generated stochastic paths is <strong>${-(stats.mean_max_drawdown || 0).toFixed(2)}%</strong>.</p>
        </div>
    `;
}

// Export for manual initialization
export { runMonteCarloAnalysis };

