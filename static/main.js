import { deps } from './config.js';
import { handleLogout, checkAuthStatus, updateAuthUI, loadSessionState, showWelcomeMessage, log } from './auth.js';
import { initialize as initializeDOM } from './dom.js';
import { initialize as initializeEvents } from './events.js';
import { setupSidebar } from './sidebar.js';
import { initializeChat, updateChatContextIndicator } from './chat.js';
import { fetchData, loadStockDatabase } from './data.js';
import { initializePortfolio } from './portfolio.js';
import { initializeBacktesting } from './backtesting.js';
import { hideAutocomplete, selectStock } from './autocomplete.js';
import { initializeMonteCarlo } from './monte_carlo.js';
import { initializeDataInformatics } from './data_informatics.js';
import { initializeDataTransparency } from './data_transparency.js';
import './replay.js';
import './forward_test.js';

function setupKeyboardShortcuts() {
    document.addEventListener('keydown', (e) => {
        // Cmd/Ctrl + K - Focus search
        if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
            e.preventDefault();
            const searchInput = document.getElementById('symbol');
            searchInput?.focus();
            searchInput?.select();
        }
        
        // / - Focus search (when not in input)
        if (e.key === '/' && !['INPUT', 'TEXTAREA'].includes(document.activeElement.tagName)) {
            e.preventDefault();
            const searchInput = document.getElementById('symbol');
            searchInput?.focus();
            searchInput?.select();
        }
    });
}

async function init() {
    log.info('Initializing application...');

    // Step 1: Populate the dependency container.
    deps.log = log;
    deps.updateAuthUI = updateAuthUI;

    // Step 2: Load critical data.
    log.debug('Step 2: Loading stock database...');
    await loadStockDatabase();

    // Step 3: Initialize UI now that the DOM is ready.
    log.debug('Step 3: Caching DOM elements...');
    initializeDOM();

    // Step 4: Load session and check auth status to update the UI early.
    log.debug('Step 4: Loading local session state and checking auth...');
    loadSessionState();
    const isAuthenticated = await checkAuthStatus();

    // Step 5: Initialize UI components and event listeners.
    log.debug('Step 5: Initializing UI components and event listeners...');
    initializeEvents();
    setupSidebar();
    initializeChat();
    initializePortfolio();
    initializeBacktesting();
    initializeMonteCarlo();
    initializeDataInformatics();
    initializeDataTransparency();
    setupKeyboardShortcuts();

    // Step 6: If authenticated and no symbol is selected, show the welcome message.
    if (isAuthenticated && !deps.STATE.currentSymbol) {
        showWelcomeMessage();
    }
    log.info('✅ Application initialized successfully.');
}

// Make selectStock function global for event handlers
window.selectStock = selectStock;

// Update footer effective date on load
function updateFooterDate() {
    const footerDateEl = document.getElementById('footer-effective-date');
    if (footerDateEl) {
        // Calculate effective date (31 days ago for SEBI compliance)
        const today = new Date();
        const effectiveDate = new Date(today);
        effectiveDate.setDate(today.getDate() - 31);
        
        const dateStr = effectiveDate.toLocaleDateString('en-IN', {
            year: 'numeric',
            month: 'short',
            day: 'numeric'
        });
        
        footerDateEl.textContent = `Effective Date: ${dateStr}`;
    }
}

document.addEventListener('DOMContentLoaded', () => {
    init();
    updateFooterDate();
});
