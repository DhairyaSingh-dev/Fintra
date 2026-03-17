/**
 * @jest-environment jsdom
 */

import { validateBacktestResults, storeBacktestResults } from '../backtesting.js';

describe('Backtest Results Validation', () => {
    const requiredKeys = [
        'final_portfolio_value',
        'market_buy_hold_value',
        'strategy_return_pct',
        'market_return_pct',
        'sharpe_ratio',
        'max_drawdown_pct'
    ];

    test('validates complete results with all required keys', () => {
        const completeResults = {
            final_portfolio_value: 10000,
            market_buy_hold_value: 9500,
            strategy_return_pct: 10.5,
            market_return_pct: 8.2,
            sharpe_ratio: 1.2,
            max_drawdown_pct: -5.5
        };

        const result = validateBacktestResults(completeResults);
        expect(result.isValid).toBe(true);
        expect(result.missingKeys).toHaveLength(0);
    });

    test('detects missing required keys', () => {
        const incompleteResults = {
            final_portfolio_value: 10000,
            // missing other keys
        };

        const result = validateBacktestResults(incompleteResults);
        expect(result.isValid).toBe(false);
        expect(result.missingKeys.length).toBeGreaterThan(0);
    });

    test('returns error for null results', () => {
        const result = validateBacktestResults(null);
        expect(result.isValid).toBe(false);
        expect(result.error).toContain('invalid');
    });

    test('returns error for non-object results', () => {
        const result = validateBacktestResults('not an object');
        expect(result.isValid).toBe(false);
    });

    test('validates numeric values for financial metrics', () => {
        const resultsWithStrings = {
            final_portfolio_value: '10000',
            market_buy_hold_value: 9500,
            strategy_return_pct: 10.5,
            market_return_pct: 8.2,
            sharpe_ratio: 1.2,
            max_drawdown_pct: -5.5
        };

        const result = validateBacktestResults(resultsWithStrings);
        expect(result.isValid).toBe(false);
        expect(result.errors).toContain('final_portfolio_value must be numeric');
    });

    test('validates percentage ranges', () => {
        const resultsWithInvalidPct = {
            final_portfolio_value: 10000,
            market_buy_hold_value: 9500,
            strategy_return_pct: 150, // Unrealistic
            market_return_pct: 8.2,
            sharpe_ratio: 1.2,
            max_drawdown_pct: -5.5
        };

        const result = validateBacktestResults(resultsWithInvalidPct);
        expect(result.warnings).toContain('strategy_return_pct seems unusually high');
    });
});

describe('Backtest Results Storage', () => {
    beforeEach(() => {
        // Clear any stored data
        delete window.currentBacktestData;
    });

    test('stores results in window.currentBacktestData', () => {
        const results = {
            final_portfolio_value: 10000,
            strategy_return_pct: 10
        };

        storeBacktestResults(results);
        expect(window.currentBacktestData).toBeDefined();
        expect(window.currentBacktestData.final_portfolio_value).toBe(10000);
    });

    test('stored results include timestamp', () => {
        const results = { final_portfolio_value: 10000 };
        storeBacktestResults(results);

        expect(window.currentBacktestData.timestamp).toBeDefined();
        expect(new Date(window.currentBacktestData.timestamp)).toBeInstanceOf(Date);
    });

    test('overwrites previous results on new store', () => {
        storeBacktestResults({ final_portfolio_value: 10000 });
        storeBacktestResults({ final_portfolio_value: 15000 });

        expect(window.currentBacktestData.final_portfolio_value).toBe(15000);
    });
});

describe('Backtest Form Validation', () => {
    beforeEach(() => {
        document.body.innerHTML = `
            <form id="backtest-form">
                <input id="symbol-input" value="RELIANCE.NS">
                <input id="initial-capital" value="10000">
                <input id="start-date" value="2024-01-01">
                <input id="end-date" value="2024-06-01">
                <select id="strategy-select">
                    <option value="rsi" selected>RSI</option>
                </select>
                <button type="submit">Run Backtest</button>
            </form>
            <div id="backtest-error"></div>
        `;
    });

    test('validates symbol is not empty', () => {
        const symbolInput = document.getElementById('symbol-input');
        symbolInput.value = '';

        const isValid = validateBacktestForm();
        expect(isValid).toBe(false);
    });

    test('validates capital is positive number', () => {
        const capitalInput = document.getElementById('initial-capital');
        capitalInput.value = '-1000';

        const isValid = validateBacktestForm();
        expect(isValid).toBe(false);
    });

    test('validates date range is logical', () => {
        const startDate = document.getElementById('start-date');
        const endDate = document.getElementById('end-date');
        startDate.value = '2024-06-01';
        endDate.value = '2024-01-01';

        const isValid = validateBacktestForm();
        expect(isValid).toBe(false);
    });

    test('validates strategy is selected', () => {
        const strategySelect = document.getElementById('strategy-select');
        strategySelect.value = '';

        const isValid = validateBacktestForm();
        expect(isValid).toBe(false);
    });
});
