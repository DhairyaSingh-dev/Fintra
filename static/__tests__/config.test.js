/**
 * @jest-environment jsdom
 */

import { sanitizeMarkdown, getAuthHeaders } from '../config.js';

// Mock DOMPurify since it's loaded from CDN in tests
global.DOMPurify = {
    sanitize: (html, options) => {
        // Simple mock: strip script tags, but keep other HTML
        return html.replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi, '');
    }
};

// Mock marked
global.marked = {
    parse: (markdown) => {
        // Very simple markdown parser for testing
        return markdown
            .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
            .replace(/\*(.*?)\*/g, '<em>$1</em>')
            .replace(/`(.*?)`/g, '<code>$1</code>')
            .replace(/^# (.*?)$/gm, '<h1>$1</h1>')
            .replace(/^## (.*?)$/gm, '<h2>$1</h2>')
            .replace(/^### (.*?)$/gm, '<h3>$1</h3>')
            .replace(/\|\|/g, '||') // placeholder
            .replace(/\|(.*?)\|/g, '<tr><td>$1</td></tr>'); // Simplified table
    }
};

describe('sanitizeMarkdown', () => {
    beforeEach(() => {
        // Clear any global state
        document.body.innerHTML = '';
    });

    test('blocks script tags', () => {
        const input = '<script>alert("xss")</script>';
        const output = sanitizeMarkdown(input);
        expect(output).not.toContain('<script>');
        expect(output).not.toContain('alert');
    });

    test('allows markdown tables', () => {
        const input = '| A | B |\n|---|---|\n| 1 | 2 |';
        const output = sanitizeMarkdown(input);
        expect(output).toContain('<table>') || expect(output).toContain('<tr>');
    });

    test('allows code blocks', () => {
        const input = '```python\nprint("hello")\n```';
        const output = sanitizeMarkdown(input);
        expect(output).toContain('<code>');
    });

    test('blocks iframe tags', () => {
        const input = '<iframe src="evil.com"></iframe>';
        const output = sanitizeMarkdown(input);
        expect(output).not.toContain('<iframe>');
    });

    test('allows basic HTML markup', () => {
        const input = '**bold** and *italic*';
        const output = sanitizeMarkdown(input);
        expect(output).toContain('<strong>');
        expect(output).toContain('<em>');
    });

    test('handles empty string', () => {
        const output = sanitizeMarkdown('');
        expect(output).toBe('');
    });

    test('handles non-string input', () => {
        const output = sanitizeMarkdown(null);
        expect(output).toBe('');
    });

    test('blocks img onerror attribute', () => {
        const input = '<img src="x" onerror="alert(1)">';
        const output = sanitizeMarkdown(input);
        expect(output).not.toContain('onerror');
        expect(output).not.toContain('alert');
    });

    test('allows href attribute in links', () => {
        const input = '[link](https://example.com)';
        const output = sanitizeMarkdown(input);
        expect(output).toContain('href');
    });
});

describe('getAuthHeaders', () => {
    let setItemSpy;

    beforeEach(() => {
        // Reset STATE between tests
        if (typeof STATE !== 'undefined') {
            STATE.authToken = null;
        }
        localStorage.clear();

        // Spy on localStorage
        setItemSpy = jest.spyOn(Storage.prototype, 'setItem');
        setItemSpy.mockClear();
    });

    afterEach(() => {
        if (setItemSpy) {
            setItemSpy.mockRestore();
        }
    });

    test('returns empty object when no token', () => {
        if (typeof STATE !== 'undefined') {
            STATE.authToken = null;
        }
        const headers = getAuthHeaders();
        expect(headers).toEqual({});
    });

    test('returns access token only when no refresh token', () => {
        if (typeof STATE !== 'undefined') {
            STATE.authToken = 'access123';
        }
        localStorage.removeItem('refreshToken');
        const headers = getAuthHeaders();
        expect(headers.Authorization).toBe('Bearer access123');
    });

    test('returns dual token format when refresh token exists', () => {
        if (typeof STATE !== 'undefined') {
            STATE.authToken = 'access123';
        }
        localStorage.setItem('refreshToken', 'refresh456');
        const headers = getAuthHeaders();
        expect(headers.Authorization).toBe('Bearer access123:refresh456');
    });

    test('refresh token is optional', () => {
        if (typeof STATE !== 'undefined') {
            STATE.authToken = 'tokenonly';
        }
        // Don't set refreshToken
        const headers = getAuthHeaders();
        expect(headers.Authorization).toBe('Bearer tokenonly');
    });
});
