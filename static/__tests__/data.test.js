/**
 * @jest-environment jsdom
 */

import { fetchStockData, handleAuthError, updateProgress } from '../data.js';
import { getAuthHeaders } from '../config.js';

// Mock fetch
global.fetch = jest.fn();

describe('Data Fetching with Auth', () => {
    beforeEach(() => {
        fetch.mockClear();
        localStorage.clear();
    });

    test('includes auth headers in request', async () => {
        const mockToken = 'test-token-123';
        localStorage.setItem('authToken', mockToken);

        fetch.mockResolvedValueOnce({
            ok: true,
            json: () => Promise.resolve({ data: [] })
        });

        await fetchStockData('RELIANCE.NS');

        expect(fetch).toHaveBeenCalledWith(
            expect.any(String),
            expect.objectContaining({
                headers: expect.objectContaining({
                    'Authorization': expect.stringContaining('Bearer')
                })
            })
        );
    });

    test('handles 401 by showing auth required message', async () => {
        fetch.mockResolvedValueOnce({
            ok: false,
            status: 401,
            json: () => Promise.resolve({ error: 'Authentication required' })
        });

        const result = await fetchStockData('RELIANCE.NS');

        expect(result.error).toContain('Authentication');
        expect(handleAuthError).toHaveBeenCalledWith(401);
    });

    test('handles network errors gracefully', async () => {
        fetch.mockRejectedValueOnce(new Error('Network error'));

        const result = await fetchStockData('RELIANCE.NS');

        expect(result.error).toBeDefined();
        expect(result.data).toBeNull();
    });

    test('handles timeout errors', async () => {
        fetch.mockRejectedValueOnce(new Error('Timeout'));

        const result = await fetchStockData('RELIANCE.NS', { timeout: 5000 });

        expect(result.error).toContain('timeout');
    });

    test('retries on transient failures', async () => {
        fetch
            .mockRejectedValueOnce(new Error('Network error'))
            .mockResolvedValueOnce({
                ok: true,
                json: () => Promise.resolve({ data: [] })
            });

        const result = await fetchStockData('RELIANCE.NS', { retries: 1 });

        expect(fetch).toHaveBeenCalledTimes(2);
        expect(result.data).toBeDefined();
    });
});

describe('Progress Updates', () => {
    beforeEach(() => {
        document.body.innerHTML = `
            <div id="loading-progress" style="display: none;">
                <div id="progress-bar"></div>
                <span id="progress-text">0%</span>
            </div>
        `;
    });

    test('shows loading progress at 10%', () => {
        updateProgress(10);
        const progressText = document.getElementById('progress-text');
        expect(progressText.textContent).toBe('10%');
    });

    test('shows loading progress at 30%', () => {
        updateProgress(30);
        const progressBar = document.getElementById('progress-bar');
        expect(progressBar.style.width).toBe('30%');
    });

    test('shows loading progress at 60%', () => {
        updateProgress(60);
        const progressText = document.getElementById('progress-text');
        expect(progressText.textContent).toBe('60%');
    });

    test('shows loading progress at 85%', () => {
        updateProgress(85);
        const progressBar = document.getElementById('progress-bar');
        expect(progressBar.style.width).toBe('85%');
    });

    test('shows loading progress at 100%', () => {
        updateProgress(100);
        const container = document.getElementById('loading-progress');
        expect(container.style.display).toBe('none');
    });

    test('hides progress when complete', () => {
        updateProgress(100, { hideOnComplete: true });
        const container = document.getElementById('loading-progress');
        expect(container.style.display).toBe('none');
    });
});

describe('Auth Error Handling', () => {
    beforeEach(() => {
        document.body.innerHTML = `
            <div id="auth-modal" style="display: none;"></div>
            <div id="error-message"></div>
        `;
        localStorage.clear();
    });

    test('shows auth modal on 401', () => {
        handleAuthError(401);
        const modal = document.getElementById('auth-modal');
        expect(modal.style.display).toBe('block');
    });

    test('clears stored tokens on 401', () => {
        localStorage.setItem('authToken', 'old-token');
        handleAuthError(401);
        expect(localStorage.getItem('authToken')).toBeNull();
    });

    test('shows error message on 403', () => {
        handleAuthError(403);
        const errorDiv = document.getElementById('error-message');
        expect(errorDiv.textContent).toContain('forbidden');
    });

    test('redirects to login on auth failure', () => {
        delete window.location;
        window.location = { href: '' };

        handleAuthError(401, { redirect: true });
        expect(window.location.href).toContain('/login');
    });
});

describe('Data Validation', () => {
    test('validates OHLCV data structure', () => {
        const validData = {
            dates: ['2024-01-01', '2024-01-02'],
            open: [100, 101],
            high: [105, 106],
            low: [99, 100],
            close: [101, 102],
            volume: [1000, 2000]
        };

        const result = validateDataStructure(validData);
        expect(result.isValid).toBe(true);
    });

    test('rejects data with missing columns', () => {
        const invalidData = {
            dates: ['2024-01-01'],
            open: [100]
            // missing other columns
        };

        const result = validateDataStructure(invalidData);
        expect(result.isValid).toBe(false);
        expect(result.missing).toContain('high');
    });

    test('rejects data with mismatched array lengths', () => {
        const invalidData = {
            dates: ['2024-01-01', '2024-01-02'],
            open: [100],
            high: [105, 106],
            low: [99, 100],
            close: [101, 102],
            volume: [1000, 2000]
        };

        const result = validateDataStructure(invalidData);
        expect(result.isValid).toBe(false);
        expect(result.error).toContain('length');
    });

    test('validates numeric values in data', () => {
        const dataWithStrings = {
            dates: ['2024-01-01'],
            open: ['not-a-number'],
            high: [105],
            low: [99],
            close: [101],
            volume: [1000]
        };

        const result = validateDataStructure(dataWithStrings);
        expect(result.isValid).toBe(false);
    });
});

describe('Cache Management', () => {
    test('caches fetched data', async () => {
        const mockData = { symbol: 'RELIANCE.NS', data: [] };
        fetch.mockResolvedValueOnce({
            ok: true,
            json: () => Promise.resolve(mockData)
        });

        await fetchStockData('RELIANCE.NS', { cache: true });
        const cached = localStorage.getItem('cache_RELIANCE.NS');
        expect(cached).toBeDefined();
    });

    test('returns cached data when available', async () => {
        const cachedData = { symbol: 'RELIANCE.NS', data: [1, 2, 3] };
        localStorage.setItem('cache_RELIANCE.NS', JSON.stringify({
            data: cachedData,
            timestamp: Date.now()
        }));

        const result = await fetchStockData('RELIANCE.NS', { cache: true });
        expect(result.data).toEqual(cachedData);
        expect(fetch).not.toHaveBeenCalled();
    });

    test('bypasses cache when stale', async () => {
        const staleData = { symbol: 'RELIANCE.NS', data: [] };
        localStorage.setItem('cache_RELIANCE.NS', JSON.stringify({
            data: staleData,
            timestamp: Date.now() - 86400000 // 1 day old
        }));

        fetch.mockResolvedValueOnce({
            ok: true,
            json: () => Promise.resolve({ symbol: 'RELIANCE.NS', data: [1] })
        });

        await fetchStockData('RELIANCE.NS', { cache: true, cacheDuration: 3600000 });
        expect(fetch).toHaveBeenCalled();
    });
});
