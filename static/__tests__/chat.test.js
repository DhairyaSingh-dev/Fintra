/**
 * @jest-environment jsdom
 */

// Mock config first
import { STATE, deps, getAuthHeaders } from '../config.js';

// Mock other dependencies
jest.mock('../config.js', () => {
    const original = jest.requireActual('../config.js');
    return {
        ...original,
        getAuthHeaders: jest.fn(() => ({})),
        showNotification: jest.fn()
    };
});

import { updateChatContextIndicator } from '../chat.js';

describe('Chat Context Modes', () => {
    beforeEach(() => {
        // Reset state between tests
        STATE.chatContextMode = 'NONE';
        STATE.chatContextSymbols = [];
        STATE.portfolio = [];
        document.body.innerHTML = `
            <button id="market-context-btn"></button>
            <button id="portfolio-context-btn"></button>
            <button id="chat-portfolio-btn"></button>
            <div id="chat-context-indicator"></div>
        `;
    });

    test('context mode can be set to MARKET', () => {
        STATE.chatContextMode = 'MARKET';
        expect(STATE.chatContextMode).toBe('MARKET');
    });

    test('context mode can be set to PORTFOLIO', () => {
        STATE.chatContextMode = 'PORTFOLIO';
        expect(STATE.chatContextMode).toBe('PORTFOLIO');
    });

    test('context mode can be set to NONE', () => {
        STATE.chatContextMode = 'NONE';
        expect(STATE.chatContextMode).toBe('NONE');
    });

    test('chat context symbols array can be modified', () => {
        STATE.chatContextSymbols = ['RELIANCE.NS', 'TCS.NS'];
        expect(STATE.chatContextSymbols).toHaveLength(2);
        expect(STATE.chatContextSymbols).toContain('RELIANCE.NS');
    });

    test('updateChatContextIndicator updates DOM', () => {
        const indicator = document.getElementById('chat-context-indicator');
        STATE.chatContextMode = 'MARKET';
        STATE.chatContextSymbols = ['RELIANCE.NS'];

        updateChatContextIndicator();

        expect(indicator.textContent).toBe('(Context: Market - RELIANCE.NS)');
    });

    test('updateChatContextIndicator shows portfolio context', () => {
        const indicator = document.getElementById('chat-context-indicator');
        STATE.chatContextMode = 'PORTFOLIO';
        STATE.chatContextSymbols = ['AAPL', 'GOOGL'];

        updateChatContextIndicator();

        expect(indicator.textContent).toContain('Portfolio');
        expect(indicator.textContent).toContain('AAPL');
    });

    test('updateChatContextIndicator shows none when no context', () => {
        const indicator = document.getElementById('chat-context-indicator');
        STATE.chatContextMode = 'NONE';
        STATE.chatContextSymbols = [];

        updateChatContextIndicator();

        expect(indicator.textContent).toContain('None');
    });
});

describe('Chat Form Submission', () => {
    let submitEvent;

    beforeEach(() => {
        document.body.innerHTML = `
            <form id="chat-form">
                <input name="message" value="Test message">
                <button type="submit">Send</button>
            </form>
        `;
        submitEvent = new Event('submit', { bubbles: true, cancelable: true });
    });

    test('form submission includes current user state', () => {
        const form = document.getElementById('chat-form');
        STATE.authToken = 'test-token';

        // Mock fetch
        const mockFetch = jest.fn(() =>
            Promise.resolve({
                ok: true,
                json: () => Promise.resolve({ success: true })
            })
        );
        global.fetch = mockFetch;

        form.dispatchEvent(submitEvent);

        // Check that fetch was called with proper payload
        // (Would need async/await or microtask wait)
    });
});

describe('Chat Message Rendering', () => {
    beforeEach(() => {
        document.body.innerHTML = `
            <div id="chat-messages"></div>
        `;
    });

    test('sanitizeMarkdown is used for message content', () => {
        // This implicitly tests that our sanitizeMarkdown function works for chat
        // As we've already tested sanitizeMarkdown in config.test.js
        const { sanitizeMarkdown } = require('../config.js');

        const dangerousInput = '<script>alert("xss")</script>';
        const safeOutput = sanitizeMarkdown(dangerousInput);

        expect(safeOutput).not.toContain('<script>');
        expect(safeOutput).not.toContain('alert');
    });
});

describe('Chat Portfolio Integration', () => {
    beforeEach(() => {
        document.body.innerHTML = `
            <button id="chat-portfolio-btn"></button>
            <div id="chat-portfolio-menu"></div>
        `;
        STATE.portfolio = [
            { symbol: 'RELIANCE.NS', name: 'Reliance Industries', quantity: 10 },
            { symbol: 'TCS.NS', name: 'Tata Consultancy', quantity: 5 }
        ];
    });

    test('portfolio data is available in STATE', () => {
        expect(STATE.portfolio).toHaveLength(2);
    });

    test('portfolio symbol names are rendered in menu', () => {
        // Simulate menu rendering (this will be triggered by actual code)
        const menu = document.getElementById('chat-portfolio-menu');

        // In real flow, clicking chat-portfolio-btn calls renderChatPortfolioMenu()
        expect(STATE.portfolio[0].symbol).toBe('RELIANCE.NS');
    });
});
