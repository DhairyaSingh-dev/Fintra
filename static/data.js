// ==================== DATA FETCHING ====================
import { deps, getAuthHeaders } from './config.js';
import { showLoading, hideLoading, hideError, showError, updateLoadingProgress } from './dom.js';
import { displayData } from './display.js';
import { updateAuthUI } from './auth.js';
import { updateAnalysisDataInfo } from './data_transparency.js';
 
const { CONFIG, STATE } = deps;

export async function loadStockDatabase() {
    try {
        const response = await fetch('stock-data.json');
        const data = await response.json();
        STATE.stockDatabase = data.stocks || [];
        deps.log.info(`Loaded ${STATE.stockDatabase.length} stocks into database.`);
    } catch (error) {
        console.error('❌ Error loading stock data:', error);
    }
}

export async function fetchData() {
    if (!STATE.currentSymbol) return;
    deps.log.debug(`Fetching data for symbol: ${STATE.currentSymbol}`);

    showLoading();
    hideError();
    document.getElementById('output').innerHTML = '';

    try {
        // Phase 1: Sending request (0-20%)
        updateLoadingProgress(10, 'Connecting to server...');
        
        const response = await fetch(`${CONFIG.API_BASE_URL}/get_data`, {
            method: "POST",
            credentials: "include",
            headers: {
                "Content-Type": "application/json",
                ...getAuthHeaders()
            },
            body: JSON.stringify({
                symbol: STATE.currentSymbol
            })
        });

        if (!response.ok) {
            if (response.status === 401) {
                showError('Authentication Required. Please sign in to view data.');
                updateAuthUI();
                return;
            }
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        // Phase 2: Receiving data (20-50%)
        updateLoadingProgress(30, 'Fetching market data...');
        
        const data = await response.json();

        if (data.error) {
            showError(data.error);
            deps.log.warn(`API returned an error for ${STATE.currentSymbol}:`, data.error);
        } else {
            // Phase 3: Processing indicators (50-80%)
            updateLoadingProgress(60, 'Calculating technical indicators (MA, RSI, MACD)...');
            
            // Phase 4: AI Analysis (80-95%)
            updateLoadingProgress(85, 'Generating AI analysis...');
            
            displayData(data);
            
            // Phase 5: Finalizing (95-100%)
            updateLoadingProgress(100, 'Complete!');
            
            // Add transparency indicators for data lag
            if (data.sebi_compliance) {
                updateAnalysisDataInfo(data);
            }
            deps.log.debug(`Successfully displayed data for ${STATE.currentSymbol}`);
        }

    } catch (error) {
        deps.log.error('Fetch error:', error);
        showError(`Failed to fetch data for ${STATE.currentSymbol}. Please try another symbol.`);
    } finally {
        // Small delay to show 100% before hiding
        setTimeout(() => {
            hideLoading();
        }, 300);
    }
}
