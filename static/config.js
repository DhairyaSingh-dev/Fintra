// ==================== CONFIGURATION & CONSTANTS ====================
const IS_LOCALHOST = window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1';

// Production Backend URL
const BACKEND_ORIGIN = 'https://stock-dashboard-fqtn.onrender.com';

// Use a relative path for local dev to go through the proxy, and a full URL for production.
const API_BASE_URL = IS_LOCALHOST ? '/api' : `${BACKEND_ORIGIN}/api`;

// Expose globally for non-module scripts (like landing.js)
window.API_BASE_URL = API_BASE_URL;

export const CONFIG = {
    API_BASE_URL: API_BASE_URL,
    DEBOUNCE_DELAY: 300,
    MAX_AUTOCOMPLETE_ITEMS: 8,
    MAX_CHART_POINTS: 30,
    SESSION_STORAGE_KEY: 'userSession',
    OAUTH_STATE_KEY: 'oauthState'
};

export const STATE = {
    stockDatabase: [],
    selectedIndex: -1,
    filteredStocks: [],
    isSidebarCollapsed: false,
    charts: { ohlcv: null, rsi: null, movingAverages: null, macd: null },
    currentSessionId: generateSessionId(),
    chatContextSymbols: [], // New: For multi-context chat
    currentSymbol: null,
    isLoading: false,
    isAuthenticated: false,
    authToken: null, // NEW: For storing the JWT
    user: null,
    chatHistory: [], // Add chat history to the global state
    portfolio: [] // Initialize portfolio state
};

// Export helper for auth headers
export function getAuthHeaders() {
    if (STATE.authToken) {
        // Include both access and refresh tokens in Authorization header
        // Format: "Bearer access_token:refresh_token"
        const refreshToken = localStorage.getItem('refreshToken') || '';
        if (refreshToken) {
            return { 'Authorization': `Bearer ${STATE.authToken}:${refreshToken}` };
        }
        return { 'Authorization': `Bearer ${STATE.authToken}` };
    }
    return {};
}


export const DOM = {};

export let sessionTimerInterval = null;

// ==================== DEPENDENCY CONTAINER ====================
// This object will hold all shared state and functions, acting as a
// centralized service locator to simplify dependency management.
export const deps = {
    STATE,
    DOM,
    CONFIG
};

// ==================== UTILITY FUNCTIONS ====================
export function generateSessionId() {
    return `session_${Math.random().toString(36).substr(2, 9)}_${Date.now()}`;
}

/**
 * Checks if required DOM elements exist in the DOM container.
 * Throws a detailed error if a dependency is missing.
 * @param {string} moduleName - The name of the module checking its dependencies.
 * @param {string[]} requiredDomElements - An array of keys to check for in the DOM object.
 */
export function checkDependencies(moduleName, requiredDomElements) {
    for (const key of requiredDomElements) {
        if (!DOM[key]) {
            throw new Error(`[${moduleName}] Missing DOM dependency: 'DOM.${key}'. Check if the element with id="${key}" exists in the HTML or is created before this module is initialized.`);
        }
    }
}

export function debounce(func, wait) {
    let timeout;
    return (...args) => {
        clearTimeout(timeout);
        timeout = setTimeout(() => func(...args), wait);
    };
}

export function formatPrice(price) { 
    return price != null ? `$${price.toFixed(2)}` : 'N/A'; 
}

export function formatNumber(num) { 
    return num != null ? num.toLocaleString() : 'N/A'; 
}

export function getRsiColor(rsi) {
    if (rsi == null) return '#6b7280';
    if (rsi > 70) return '#ef4444';
    if (rsi < 30) return '#10b981';
    return '#F0F4F8'; // Use a lighter color for neutral on dark backgrounds
}

export function getRsiBackground(rsi) {
    if (rsi == null) return '#f3f4f6';
    if (rsi > 70) return '#fef2f2';
    if (rsi < 30) return '#f0fdf4';
    return '#f8fafc';
}
