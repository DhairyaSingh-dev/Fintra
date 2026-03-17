/**
 * Data Informatics Module
 * Displays SEBI compliance information and data availability
 */

import { CONFIG, getAuthHeaders } from './config.js';

/**
 * Fetch and display data availability information
 */
export async function loadDataInformatics() {
    try {
        const response = await fetch(`${CONFIG.API_BASE_URL}/data/availability`, {
            method: 'GET',
            headers: getAuthHeaders()
        });
        
        if (!response.ok) {
            console.warn('Failed to load data availability');
            return null;
        }
        
        const data = await response.json();
        displayDataInformatics(data);
        return data;
        
    } catch (error) {
        console.error('Error loading data informatics:', error);
        return null;
    }
}

/**
 * Display data informatics in the UI
 */
function displayDataInformatics(data) {
    const container = document.getElementById('data-informatics');
    if (!container) return;
    
    if (!data.available) {
        container.innerHTML = `
            <div class="data-informatics warning">
                <h4>📊 Data Availability</h4>
                <p class="warning-text">${data.message || 'Data unavailable'}</p>
                <p>SEBI Compliance: ${data.lag_days || 30}-day lag enforced</p>
            </div>
        `;
        return;
    }
    
    const statusClass = data.needs_manual_lag ? 'warning' : 'success';
    const statusIcon = data.needs_manual_lag ? '⚠️' : '✅';
    const lagWarning = data.needs_manual_lag ? 
        `<p class="lag-warning">⚠️ Data is ${data.days_behind_lag} days behind the SEBI lag requirement</p>` : '';
    
    container.innerHTML = `
        <div class="data-informatics ${statusClass}">
            <div class="informatics-header">
                <h4>📊 Data Informatics</h4>
                <span class="compliance-badge ${statusClass}">${statusIcon} SEBI Compliant</span>
            </div>
            
            <div class="info-grid">
                <div class="info-item">
                    <span class="label">Data Range:</span>
                    <span class="value">${data.first_date} to ${data.last_date}</span>
                </div>
                <div class="info-item">
                    <span class="label">Total History:</span>
                    <span class="value">${data.total_days} trading days</span>
                </div>
                <div class="info-item">
                    <span class="label">SEBI Compliance:</span>
                    <span class="value">${data.lag_days}-day mandatory lag</span>
                </div>
                <div class="info-item">
                    <span class="label">Effective Date:</span>
                    <span class="value">${data.effective_last_date}</span>
                </div>
                <div class="info-item">
                    <span class="label">Data Freshness:</span>
                    <span class="value">${data.data_freshness_days} days old</span>
                </div>
            </div>
            
            ${lagWarning}
            
            <div class="compliance-notice">
                <p>🔒 <strong>Regulatory Notice:</strong> This platform maintains a strict ${data.lag_days}-day data lag 
                in accordance with SEBI regulations. No current or recent market data is displayed. 
                All analysis is based on historical data only.</p>
            </div>
        </div>
    `;
}

/**
 * Add compliance notice to analysis outputs
 */
export function addComplianceNotice(containerId) {
    const container = document.getElementById(containerId);
    if (!container) return;
    
    const notice = document.createElement('div');
    notice.className = 'sebi-compliance-notice';
    notice.innerHTML = `
        <div class="notice-content">
            <span class="notice-icon">🔒</span>
            <p><strong>SEBI Compliance Notice:</strong> This analysis uses historical data with a mandatory 31-day lag. 
            No current market data or investment advice is provided. For educational purposes only. 
            Past performance does not guarantee future results.</p>
        </div>
    `;
    
    container.appendChild(notice);
}

/**
 * Update backtest form with data availability
 */
export function updateBacktestDateRange(data) {
    if (!data || !data.available) return;
    
    const startDateInput = document.getElementById('start-date');
    const endDateInput = document.getElementById('end-date');
    
    if (startDateInput) {
        startDateInput.min = data.first_date;
        startDateInput.max = data.effective_last_date;
        startDateInput.value = data.first_date;
    }
    
    if (endDateInput) {
        endDateInput.min = data.first_date;
        endDateInput.max = data.effective_last_date;
        endDateInput.value = data.effective_last_date;
    }
    
    // Add data range indicator
    const form = document.getElementById('backtesting-form');
    if (form) {
        let rangeIndicator = form.querySelector('.data-range-indicator');
        if (!rangeIndicator) {
            rangeIndicator = document.createElement('div');
            rangeIndicator.className = 'data-range-indicator';
            form.insertBefore(rangeIndicator, form.firstChild);
        }
        rangeIndicator.innerHTML = `
            <span class="range-info">📅 Available Data: ${data.first_date} to ${data.effective_last_date} 
            (${data.total_days} days) | SEBI Lag: ${data.lag_days} days</span>
        `;
    }
}

/**
 * Initialize data informatics on page load
 */
export function initializeDataInformatics() {
    // Load on dashboard
    const dashboard = document.getElementById('dashboard-view');
    if (dashboard) {
        loadDataInformatics();
    }
    
    // Load on backtesting page
    const backtestingTab = document.getElementById('backtesting-tab');
    if (backtestingTab) {
        backtestingTab.addEventListener('click', async () => {
            const data = await loadDataInformatics();
            if (data) {
                updateBacktestDateRange(data);
            }
        });
    }
}

export default {
    loadDataInformatics,
    addComplianceNotice,
    updateBacktestDateRange,
    initializeDataInformatics
};
