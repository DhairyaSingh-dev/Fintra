// ==================== CHATBOT ====================
import { deps, generateSessionId, checkDependencies, sanitizeMarkdown } from './config.js';
import { saveSessionState } from './auth.js';
import { showNotification } from './notifications.js';
import { showAuthOverlay } from './auth.js';

const { STATE, DOM, CONFIG } = deps;

let portfolioPositions = [];

// Context modes
const CONTEXT_MODES = {
    NONE: 'none',
    MARKET: 'market',  // 📈 Current stock
    PORTFOLIO: 'portfolio'  // 📁 Portfolio
};

let currentContextMode = CONTEXT_MODES.NONE;
let selectedPortfolioContext = null;

export function initializeChat() {
    // Defensively check that all required DOM elements are available before proceeding.
    checkDependencies('initializeChat', [
        'chatToggle', 'chatClose', 'chatSend', 'chatInput', 'chatRefresh', 'chatMessages'
    ]);

    DOM.chatToggle.addEventListener('click', toggleChatWindow);
    DOM.chatClose.addEventListener('click', toggleChatWindow);
    DOM.chatSend.addEventListener('click', handleChatSubmit);
    DOM.chatInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') handleChatSubmit();
    });
    DOM.chatRefresh.addEventListener('click', refreshChatContext);

    // Add emoji context toggles to chat header
    addContextToggles();

    // Fetch portfolio positions for potential context
    fetchPortfolioPositions();

    // Welcome message - properly styled without dark box
    const welcomeDiv = document.createElement('div');
    welcomeDiv.className = 'chat-welcome-message';
    welcomeDiv.innerHTML = `
        <div class="chat-welcome-content">
            <div class="chat-welcome-icon">👋</div>
            <div class="chat-welcome-title">Welcome to Fintra!</div>
            <div class="chat-welcome-text">
                I'm here to help you learn about technical analysis and market patterns using historical data.
            </div>
            <div class="chat-welcome-hint">
                Select a context above (📈 Market or 📁 Portfolio) or just chat with me!
            </div>
        </div>
    `;
    DOM.chatMessages.innerHTML = '';
    DOM.chatMessages.appendChild(welcomeDiv);
    
    updateChatContextIndicator();
    STATE.chatHistory = [];
}

function addContextToggles() {
    const chatHeader = document.querySelector('.chat-header') || document.getElementById('chat-header');
    if (!chatHeader) {
        console.warn('Chat header not found');
        return;
    }

    // Create context toggles container
    const togglesContainer = document.createElement('div');
    togglesContainer.id = 'context-toggles';
    togglesContainer.style.cssText = `
        display: flex;
        gap: 12px;
        align-items: center;
        margin-right: 10px;
    `;

    togglesContainer.innerHTML = `
        <div class="context-toggle-wrapper">
            <button id="market-toggle" class="context-toggle" title="Current Stock Analysis">
                📈
            </button>
            <div class="context-tooltip">
                <strong>Market Context</strong>
                <span>Analyze the currently selected stock</span>
            </div>
        </div>
        <div class="context-toggle-wrapper">
            <button id="portfolio-toggle" class="context-toggle" title="Portfolio Analysis">
                📁
            </button>
            <div class="context-tooltip">
                <strong>Portfolio Context</strong>
                <span>View your portfolio positions</span>
            </div>
        </div>
    `;

  // Find the flex container that holds the buttons (where close button is)
  const buttonContainer = chatHeader.querySelector('div[style*="display: flex"]');
  if (buttonContainer) {
    // Insert before the first child of the button container (usually the portfolio button)
    const firstButton = buttonContainer.firstElementChild;
    if (firstButton) {
      buttonContainer.insertBefore(togglesContainer, firstButton);
    } else {
      buttonContainer.appendChild(togglesContainer);
    }
  } else {
    // Fallback: append to chat header
    chatHeader.appendChild(togglesContainer);
  }

    // Add event listeners
    document.getElementById('market-toggle').addEventListener('click', () => toggleContext(CONTEXT_MODES.MARKET));
    document.getElementById('portfolio-toggle').addEventListener('click', () => toggleContext(CONTEXT_MODES.PORTFOLIO));
}

function toggleContext(mode) {
    const marketBtn = document.getElementById('market-toggle');
    const portfolioBtn = document.getElementById('portfolio-toggle');

    // If clicking already active mode, turn it off
    if (currentContextMode === mode) {
        currentContextMode = CONTEXT_MODES.NONE;
        selectedPortfolioContext = null;
        marketBtn?.classList.remove('active');
        portfolioBtn?.classList.remove('active');
        showNotification('Context cleared. General chat mode.', 'info');
    } else {
        // Turn off all, then turn on selected
        currentContextMode = mode;
        marketBtn?.classList.remove('active');
        portfolioBtn?.classList.remove('active');

        if (mode === CONTEXT_MODES.MARKET) {
            marketBtn?.classList.add('active');
            if (STATE.currentSymbol) {
                showNotification(`Market context: ${STATE.currentSymbol}`, 'info');
            } else {
                showNotification('Market context selected. Please select a stock first.', 'warning');
            }
        } else if (mode === CONTEXT_MODES.PORTFOLIO) {
            portfolioBtn?.classList.add('active');
            showPortfolioSelector();
        }
    }

    updateChatContextIndicator();
}

function showPortfolioSelector() {
    if (portfolioPositions.length === 0) {
        showNotification('No portfolio positions available. Add positions in the Portfolio tab.', 'warning');
        currentContextMode = CONTEXT_MODES.NONE;
        document.getElementById('portfolio-toggle')?.classList.remove('active');
        return;
    }

    // Create modal for portfolio selection
    const modal = document.createElement('div');
    modal.id = 'portfolio-context-modal';
    modal.style.cssText = `
        position: fixed;
        top: 0;
        left: 0;
        right: 0;
        bottom: 0;
        background: rgba(0, 0, 0, 0.7);
        display: flex;
        align-items: center;
        justify-content: center;
        z-index: 10000;
    `;

    const positionsList = portfolioPositions.map(p => `
        <div class="portfolio-context-item" data-id="${p.id}" style="
            padding: 12px;
            margin: 8px 0;
            background: #1f2937;
            border-radius: 8px;
            cursor: pointer;
            transition: all 0.2s;
            border: 2px solid transparent;
        " onmouseover="this.style.borderColor='#3b82f6'" onmouseout="this.style.borderColor='transparent'">
            <strong style="color: #3b82f6; font-size: 1.1rem;">${p.symbol}</strong>
            <span style="color: #9ca3af; margin-left: 10px;">${p.quantity} shares @ ₹${p.entry_price.toFixed(2)}</span>
        </div>
    `).join('');

    modal.innerHTML = `
        <div style="
            background: #111827;
            padding: 24px;
            border-radius: 12px;
            max-width: 400px;
            width: 90%;
            max-height: 80vh;
            overflow-y: auto;
        ">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px;">
                <h3 style="margin: 0; color: #f3f4f6;">Select Position</h3>
                <button onclick="this.closest('#portfolio-context-modal').remove()" style="
                    background: none;
                    border: none;
                    color: #9ca3af;
                    font-size: 1.5rem;
                    cursor: pointer;
                ">×</button>
            </div>
            <p style="color: #9ca3af; margin-bottom: 16px; font-size: 0.9rem;">
                Choose a position to include in our conversation:
            </p>
            ${positionsList}
        </div>
    `;

    document.body.appendChild(modal);

    // Add click handlers
    modal.querySelectorAll('.portfolio-context-item').forEach(item => {
        item.addEventListener('click', () => {
            selectedPortfolioContext = parseInt(item.dataset.id);
            const position = portfolioPositions.find(p => p.id === selectedPortfolioContext);
            modal.remove();
            showNotification(`Portfolio context: ${position?.symbol || 'Selected'}`, 'info');
            updateChatContextIndicator();
        });
    });

    // Close on background click
    modal.addEventListener('click', (e) => {
        if (e.target === modal) {
            modal.remove();
        }
    });
}

async function fetchPortfolioPositions() {
    try {
        const response = await fetch(`${CONFIG.API_BASE_URL}/portfolio/positions/list`, {
            headers: getAuthHeaders()
        });

        if (!response.ok) {
            console.warn('Failed to fetch portfolio positions');
            return;
        }

        portfolioPositions = await response.json();
    } catch (error) {
        console.error('Error fetching portfolio positions:', error);
    }
}

function toggleChatWindow() {
    DOM.chatWindow.classList.toggle('active');
}

function refreshChatContext() {
    currentContextMode = CONTEXT_MODES.NONE;
    selectedPortfolioContext = null;
    STATE.chatContextSymbols = [];
    STATE.currentSymbol = null;
    STATE.chatHistory = [];
    
    // Reset toggle buttons
    document.getElementById('market-toggle')?.classList.remove('active');
    document.getElementById('portfolio-toggle')?.classList.remove('active');
    
    updateChatContextIndicator();

    const refreshedDiv = document.createElement('div');
    refreshedDiv.className = 'chat-welcome-message';
    refreshedDiv.innerHTML = `
        <div class="chat-welcome-content">
            <div class="chat-welcome-icon" style="animation: none;">🔄</div>
            <div class="chat-welcome-title">Chat Refreshed</div>
            <div class="chat-welcome-text">
                Context has been cleared. You can start a new conversation!
            </div>
        </div>
    `;
    DOM.chatMessages.innerHTML = '';
    DOM.chatMessages.appendChild(refreshedDiv);
    showNotification('Chat context refreshed.', 'info');
}

function handleChatSubmit() {
    const text = DOM.chatInput.value.trim();
    if (!text) return;

    if (!STATE.isAuthenticated) {
        const systemMessage = appendMessage({ role: 'system', content: 'Please sign in to use the AI Chatbot.' }, true);
        setTimeout(() => {
            systemMessage?.remove();
        }, 5000);
        showAuthOverlay();
        return;
    }

    appendMessage({ role: 'user', content: text });
    DOM.chatInput.value = '';

    const typingIndicator = document.createElement('div');
    typingIndicator.className = 'msg msg-bot typing-indicator';
    typingIndicator.innerHTML = '<span></span><span></span><span></span>';
    DOM.chatMessages.appendChild(typingIndicator);
    DOM.chatMessages.scrollTop = DOM.chatMessages.scrollHeight;

    // Build context data
    const contextData = {
        query: text,
        mode: currentContextMode,
        symbol: null,
        position_id: null
    };

    if (currentContextMode === CONTEXT_MODES.MARKET && STATE.currentSymbol) {
        contextData.symbol = STATE.currentSymbol;
    } else if (currentContextMode === CONTEXT_MODES.PORTFOLIO && selectedPortfolioContext) {
        contextData.position_id = selectedPortfolioContext;
    }

    try {
        fetch(`${CONFIG.API_BASE_URL}/chat`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                ...getAuthHeaders()
            },
            body: JSON.stringify(contextData),
            credentials: 'include'
        })
        .then(async response => {
            // Handle token refresh
            if (response.status === 401) {
                const data = await response.json();
                if (data.access_token && data.refresh_token) {
                    console.log('Token refreshed by backend. Updating stored tokens.');
                    localStorage.setItem('accessToken', data.access_token);
                    localStorage.setItem('refreshToken', data.refresh_token);
                    // Retry the request with new tokens
                    return fetch(`${CONFIG.API_BASE_URL}/chat`, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            'Accept': 'application/json',
                            'Authorization': `Bearer ${data.access_token}:${data.refresh_token}`
                        },
                        body: JSON.stringify(contextData),
                        credentials: 'include'
                    });
                } else {
                    showAuthOverlay();
                    throw new Error('Unauthorized. Please sign in again.');
                }
            }
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return response.json();
        })
        .then(data => {
            if (typingIndicator && typingIndicator.parentNode) {
                typingIndicator.remove();
            }
            if (data.response) {
                appendMessage({ role: 'bot', content: data.response });
            } else {
                appendMessage({ role: 'system', content: 'Sorry, I couldn\'t get a response. Try rephrasing.' });
            }
        })
        .catch(err => {
            if (typingIndicator && typingIndicator.parentNode) {
                typingIndicator.remove();
            }
            appendMessage({ role: 'system', content: `An error occurred: ${err.message}.` });
            console.error('Chat error:', err);
        });
    } catch (err) {
        if (typingIndicator && typingIndicator.parentNode) {
            typingIndicator.remove();
        }
        appendMessage({ role: 'system', content: 'A connection error occurred. Please check your network.' });
        console.error('Chat error:', err);
    }
}

function appendMessage(message, isTemporary = false) {
    const { role, content } = message;

    if (content !== '...' && !isTemporary) {
        STATE.chatHistory.push({ role, content });
    }

    const div = document.createElement('div');
    div.className = `msg msg-${role}`;

    if (role === 'bot' || role === 'system') {
        const html = sanitizeMarkdown(content);
        div.innerHTML = html;
    } else {
        div.textContent = content;
    }

    DOM.chatMessages.appendChild(div);
    DOM.chatMessages.scrollTop = DOM.chatMessages.scrollHeight;
    return div;
}

export function updateChatContextIndicator() {
    const contextIndicator = document.getElementById('chat-context-header');
    if (!contextIndicator) return;

    let contextText = '';

    if (currentContextMode === CONTEXT_MODES.MARKET && STATE.currentSymbol) {
        contextText = `📈 Market: ${STATE.currentSymbol}`;
    } else if (currentContextMode === CONTEXT_MODES.PORTFOLIO && selectedPortfolioContext) {
        const position = portfolioPositions.find(p => p.id === selectedPortfolioContext);
        if (position) {
            contextText = `📁 Portfolio: ${position.symbol}`;
        } else {
            contextText = '📁 Portfolio: Select position';
        }
    } else {
        contextText = '💬 General Chat';
    }

    contextIndicator.textContent = contextText;
    contextIndicator.style.color = currentContextMode === CONTEXT_MODES.NONE ? '#9ca3af' : '#3b82f6';
}

// Export for external use
export { currentContextMode, CONTEXT_MODES };
