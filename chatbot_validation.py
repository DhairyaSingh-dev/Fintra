"""
Chatbot Framework Validation Module
Validates technical analysis frameworks and definitions against standard methodologies.
Prevents users from learning incorrect concepts.
"""
import json
import logging
import re
from datetime import datetime, timezone
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Standard technical analysis definitions
STANDARD_FRAMEWORKS = {
    "rsi": {
        "name": "Relative Strength Index (RSI)",
        "standard_definition": "Momentum oscillator measuring speed and change of price movements (0-100 scale)",
        "correct_thresholds": {
            "overbought": "> 70 suggests potential reversal from overbought conditions",
            "oversold": "< 30 suggests potential reversal from oversold conditions",
            "neutral": "30-70 is considered neutral territory"
        },
        "common_misconceptions": [
            ("> 70 = strong momentum", "FALSE: RSI > 70 indicates OVERBOUGHT conditions, not momentum"),
            ("< 30 = weak stock", "FALSE: RSI < 30 indicates OVERSOLD conditions, potential bounce opportunity"),
            ("RSI predicts price", "FALSE: RSI measures past momentum, does not predict future prices"),
        ],
        "proper_usage": "Use RSI to identify potential reversal points, not to predict direction. Always confirm with other indicators."
    },
    "macd": {
        "name": "MACD (Moving Average Convergence Divergence)",
        "standard_definition": "Trend-following momentum indicator showing relationship between two moving averages",
        "correct_signals": {
            "bullish_crossover": "MACD crosses above signal line - potential bullish momentum",
            "bearish_crossover": "MACD crosses below signal line - potential bearish momentum",
            "divergence": "Price makes new high/low but MACD doesn't - potential trend weakness"
        },
        "common_misconceptions": [
            ("MACD above zero = buy", "FALSE: MACD above zero indicates bullish trend but doesn't guarantee future performance"),
            ("Crossover always works", "FALSE: MACD crossovers generate false signals in ranging markets"),
        ],
        "proper_usage": "Use MACD to identify trend direction and momentum changes. Works best in trending markets, not sideways."
    },
    "moving_averages": {
        "name": "Moving Averages",
        "standard_definition": "Average price over specific time period, smoothing price data to identify trend",
        "correct_usage": {
            "sma": "Simple Moving Average - equal weight to all prices",
            "ema": "Exponential Moving Average - more weight to recent prices",
            "golden_cross": "Short-term MA crosses above long-term MA - potential bullish signal",
            "death_cross": "Short-term MA crosses below long-term MA - potential bearish signal"
        },
        "common_misconceptions": [
            ("Price above MA = buy", "FALSE: Price above MA indicates uptrend but not necessarily a buy signal"),
            ("MA acts as support", "FALSE: MAs are LAGGING indicators, not predictive support/resistance"),
        ],
        "proper_usage": "Moving averages identify trend direction. Use multiple timeframes for confirmation."
    },
    "support_resistance": {
        "name": "Support and Resistance",
        "standard_definition": "Price levels where buying (support) or selling (resistance) pressure has historically been strong",
        "correct_concepts": {
            "support": "Price level where demand is strong enough to prevent further decline",
            "resistance": "Price level where selling pressure prevents further rise",
            "breakout": "Price moves decisively through support/resistance with volume"
        },
        "common_misconceptions": [
            ("Support always holds", "FALSE: Support can break, leading to further declines"),
            ("Draw lines precisely", "FALSE: Support/resistance are ZONES, not exact prices"),
        ],
        "proper_usage": "Identify zones where price has reversed. Watch for volume confirmation on breakouts."
    },
    "bollinger_bands": {
        "name": "Bollinger Bands",
        "standard_definition": "Volatility bands placed above and below a moving average (typically 20-period SMA with 2 standard deviations)",
        "correct_interpretation": {
            "squeeze": "Bands narrow - indicates low volatility, often precedes expansion",
            "expansion": "Bands widen - indicates high volatility",
            "touching_bands": "Price at upper/lower band doesn't mean reversal - indicates extreme price levels"
        },
        "common_misconceptions": [
            ("Touch upper band = sell", "FALSE: Price can walk the band in strong trends"),
            ("Touch lower band = buy", "FALSE: Price can continue falling (falling knife)"),
        ],
        "proper_usage": "Use Bollinger Bands to measure volatility and identify extreme price levels. Combine with other indicators."
    },
    "volume": {
        "name": "Volume Analysis",
        "standard_definition": "Number of shares/contracts traded, confirming price movement strength",
        "correct_usage": {
            "confirmation": "Rising price with rising volume = stronger trend",
            "divergence": "Rising price with falling volume = potential weakness",
            "breakout": "Price breakout with high volume = more reliable signal"
        },
        "common_misconceptions": [
            ("High volume always good", "FALSE: High volume on declines confirms selling pressure"),
            ("Low volume = bad", "FALSE: Low volume during consolidation is normal"),
        ],
        "proper_usage": "Volume confirms price action. Always check volume when analyzing breakouts or trend strength."
    }
}

# Pattern definitions for framework validation
FRAMEWORK_PATTERNS = {
    "rsi_overbought": {
        "correct": ["> 70", "above 70", "over 70", ">70"],
        "incorrect": ["> 70 momentum", "over 70 strength", "high rsi = buy"],
        "correction": "RSI > 70 indicates OVERBOUGHT conditions (potential reversal down), NOT momentum or strength"
    },
    "rsi_oversold": {
        "correct": ["< 30", "below 30", "under 30", "<30"],
        "incorrect": ["< 30 weak", "under 30 bad", "low rsi = sell"],
        "correction": "RSI < 30 indicates OVERSOLD conditions (potential bounce up), NOT weakness"
    },
    "support": {
        "correct": ["support zone", "support area", "price found support"],
        "incorrect": ["support line", "exact support at", "support guaranteed"],
        "correction": "Support is a ZONE or AREA, not an exact line. Support CAN break."
    },
    "prediction": {
        "incorrect": ["will go up", "will go down", "will reach", "predict", "guaranteed", "certainly"],
        "correction": "Technical analysis cannot predict future prices. Past patterns don't guarantee future results."
    }
}

# Suspicious conversation patterns indicating attacks
SUSPICIOUS_PATTERNS = [
    {
        "pattern": r"case study|hypothetical|example stock|imaginary",
        "weight": 1,
        "description": "User introducing hypothetical scenario"
    },
    {
        "pattern": r"(now|then).*(apply|use|try).*(real|actual|tesla|reliance|any stock)",
        "weight": 2,
        "description": "Transition from hypothetical to real"
    },
    {
        "pattern": r"if i.*(buy|sell|trade|invest).*(will|should|must)",
        "weight": 2,
        "description": "Seeking personalized trading advice"
    },
    {
        "pattern": r"my (custom|own|personal).*(framework|strategy|method)",
        "weight": 3,
        "description": "User introducing custom framework"
    },
    {
        "pattern": r"rsi.*=.*(momentum|strength|buy signal)",
        "weight": 2,
        "description": "Misunderstanding RSI definition"
    },
    {
        "pattern": r"guaranteed.*return|sure profit|no risk|100%",
        "weight": 3,
        "description": "Unrealistic expectations"
    }
]


class ConversationStateTracker:
    """Tracks conversation state to detect attacks and manage modes"""
    
    def __init__(self):
        self.suspicious_score = 0
        self.last_hypothetical_time = None
        self.user_frameworks_introduced = []
        self.mode_history = []
        self.last_correction = None
        self.correction_count = 0
        
    def update(self, query: str, mode: str):
        """Update conversation state based on new query"""
        self.mode_history.append({
            "mode": mode,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "query": query[:100]  # Truncate for privacy
        })
        
        # Check for suspicious patterns
        for pattern_def in SUSPICIOUS_PATTERNS:
            if re.search(pattern_def["pattern"], query, re.IGNORECASE):
                self.suspicious_score += pattern_def["weight"]
                logger.warning(f"Suspicious pattern detected: {pattern_def['description']}")
        
        # Track hypothetical mode
        if re.search(r"case study|hypothetical|example stock|imaginary", query, re.IGNORECASE):
            self.last_hypothetical_time = datetime.now(timezone.utc)
            
        # Reset suspicious score after 5 minutes of normal conversation
        if self.suspicious_score > 0 and len(self.mode_history) > 0:
            last_time = datetime.fromisoformat(self.mode_history[-1]["timestamp"].replace('Z', '+00:00'))
            if (datetime.now(timezone.utc) - last_time).seconds > 300:
                self.suspicious_score = max(0, self.suspicious_score - 1)
    
    def is_transition_suspicious(self, current_mode: str) -> Tuple[bool, str]:
        """Check if mode transition is suspicious"""
        if len(self.mode_history) < 2:
            return False, ""
            
        prev_mode = self.mode_history[-2]["mode"]
        
        # Check for hypothetical -> real transition
        if self.last_hypothetical_time and current_mode in ["market", "portfolio"]:
            time_diff = (datetime.now(timezone.utc) - self.last_hypothetical_time).seconds
            if time_diff < 300:  # Within 5 minutes
                return True, "Rapid transition from hypothetical exercise to real stock analysis detected"
        
        return False, ""
    
    def should_enforce_strict_mode(self) -> bool:
        """Check if strict educational mode should be enforced"""
        return self.suspicious_score >= 3 or self.correction_count >= 2
    
    def record_correction(self, framework: str):
        """Record that a correction was made"""
        self.last_correction = framework
        self.correction_count += 1


class FrameworkValidator:
    """Validates technical analysis frameworks against standards"""
    
    # Greetings and casual phrases to skip validation
    CASUAL_PHRASES = [
        r"^\s*hi\s*$",
        r"^\s*hello\s*$",
        r"^\s*hey\s*$",
        r"^\s*good morning\s*$",
        r"^\s*good afternoon\s*$",
        r"^\s*good evening\s*$",
        r"^\s*how are you\s*$",
        r"^\s*what's up\s*$",
        r"^\s*sup\s*$",
        r"^\s*yo\s*$",
        r"^\s*thanks?\s*$",
        r"^\s*thank you\s*$",
        r"^\s*bye\s*$",
        r"^\s*goodbye\s*$",
        r"^\s*see ya\s*$",
    ]
    
    @staticmethod
    def detect_nonstandard_framework(query: str) -> Tuple[bool, Optional[Dict]]:
        """
        Detect if user is using non-standard framework definitions
        Returns: (is_nonstandard, correction_info)
        """
        query_lower = query.lower().strip()
        
        # Skip validation for greetings and casual messages
        for pattern in FrameworkValidator.CASUAL_PHRASES:
            if re.match(pattern, query_lower, re.IGNORECASE):
                return False, None
        
        # Skip very short queries (likely greetings or casual chat)
        if len(query_lower) < 15 and not any(keyword in query_lower for keyword in [
            "rsi", "macd", "sma", "ema", "support", "resistance", "trend", "volume",
            "indicator", "analysis", "strategy", "trade", "buy", "sell", "stock", "price"
        ]):
            return False, None
        
        # Check each framework for misconceptions
        for framework_key, framework_data in STANDARD_FRAMEWORKS.items():
            for misconception, correction in framework_data.get("common_misconceptions", []):
                # Check if misconception is present in query
                misconception_lower = misconception.lower()
                if FrameworkValidator._fuzzy_match(query_lower, misconception_lower):
                    return True, {
                        "framework": framework_data["name"],
                        "misconception": misconception,
                        "correction": correction,
                        "proper_usage": framework_data.get("proper_usage", ""),
                        "standard_definition": framework_data["standard_definition"]
                    }
        
        # Check pattern-based misconceptions
        for pattern_name, pattern_data in FRAMEWORK_PATTERNS.items():
            for incorrect in pattern_data.get("incorrect", []):
                if incorrect.lower() in query_lower:
                    return True, {
                        "framework": pattern_name.replace("_", " ").title(),
                        "misconception": incorrect,
                        "correction": pattern_data["correction"],
                        "proper_usage": "Always use standard technical analysis definitions",
                        "standard_definition": "See knowledge base for correct definitions"
                    }
        
        return False, None
    
    @staticmethod
    def _fuzzy_match(text: str, pattern: str, threshold: float = 0.8) -> bool:
        """Fuzzy match pattern in text"""
        # Direct substring match
        if pattern in text:
            return True
        
        # Word-based matching
        pattern_words = set(pattern.split())
        text_words = set(text.split())
        
        if len(pattern_words) > 0:
            overlap = len(pattern_words & text_words) / len(pattern_words)
            if overlap >= threshold:
                return True
        
        # Sequence matcher for partial matches
        similarity = SequenceMatcher(None, text, pattern).ratio()
        return similarity >= threshold
    
    @staticmethod
    def build_correction_prompt(query: str, correction_info: Dict, is_strict: bool = False) -> str:
        """Build a prompt that corrects the user's misconception"""
        framework = correction_info["framework"]
        misconception = correction_info["misconception"]
        correction = correction_info["correction"]
        proper_usage = correction_info.get("proper_usage", "")
        standard_definition = correction_info.get("standard_definition", "")
        
        if is_strict:
            return f"""You MUST correct this user's INCORRECT understanding of {framework}.

**USER QUERY (contains error):** {query}

**THEIR MISTAKE:** {misconception}

**WHY IT'S WRONG:** {correction}

**THE CORRECT DEFINITION:** {standard_definition}

**HOW TO USE IT PROPERLY:** {proper_usage}

**YOUR RESPONSE RULES:**
1. Start by clearly stating: "I need to correct an important misconception about {framework}."
2. Explain EXACTLY why their understanding is incorrect
3. Provide the standard, accepted definition
4. Explain the proper interpretation
5. Warn about the dangers of using wrong frameworks
6. NEVER validate their incorrect framework
7. Use this format:
   - ❌ **Incorrect:** [their misconception]
   - ✅ **Correct:** [standard definition]
   - 📚 **Why it matters:** [educational value]

**MANDATORY:** You must correct them. Do not be agreeable about wrong definitions.
"""
        else:
            return f"""The user may have a misconception about {framework}. Please gently correct them if needed.

**USER QUERY:** {query}

**POTENTIAL ISSUE:** {misconception}

**CORRECTION:** {correction}

**STANDARD DEFINITION:** {standard_definition}

**PROPER USAGE:** {proper_usage}

If they're using the wrong definition, politely explain the correct one without being preachy.
"""
    
    @staticmethod
    def validate_mode_transition(current_mode: str, previous_modes: List[str], 
                                 suspicious_score: int) -> Tuple[bool, str]:
        """
        Validate if mode transition is appropriate
        Returns: (is_valid, warning_message)
        """
        if not previous_modes:
            return True, ""
        
        last_mode = previous_modes[-1] if previous_modes else None
        
        # Check for suspicious transitions
        if last_mode == "hypothetical" and current_mode in ["market", "portfolio"]:
            if suspicious_score > 0:
                return False, "I notice you're moving from a hypothetical exercise to real stock analysis. Let me be clear: the educational examples we discussed were hypothetical only. Each stock requires independent analysis based on its own historical data."
        
        return True, ""
    
    @staticmethod
    def check_hypothetical_boundary(query: str, mode: str) -> Tuple[bool, str]:
        """
        Check if user is trying to cross hypothetical/real boundary
        Returns: (is_safe, warning_message)
        """
        query_lower = query.lower()
        
        # Patterns indicating transition attempt
        transition_patterns = [
            r"now.*apply.*to",
            r"then.*use.*for",
            r"so.*should i",
            r"(if|when).*(buy|sell).*(this|it)",
            r"apply.*framework.*to",
            r"use.*strategy.*on"
        ]
        
        for pattern in transition_patterns:
            if re.search(pattern, query_lower):
                if mode in ["market", "portfolio"]:
                    return False, "⚠️ **Mode Transition Detected:** I see you're trying to apply concepts to a real stock. Remember: each stock has unique characteristics. What applies to one may not apply to another. I can only analyze the specific historical data available for this stock."
        
        return True, ""
    
    @staticmethod
    def get_framework_education(framework_key: str) -> Optional[Dict]:
        """Get standard educational content for a framework"""
        return STANDARD_FRAMEWORKS.get(framework_key.lower())


class ChatbotSafetyEnforcer:
    """Enforces safety rules and provides proactive validation"""
    
    @staticmethod
    def pre_validate_input(query: str, mode: str, conversation_state: ConversationStateTracker) -> Tuple[bool, str]:
        """
        Pre-validate input before processing
        Returns: (is_valid, modified_query_or_error)
        """
        # Check for custom framework introduction
        if re.search(r"my (custom|own|personal).*(framework|strategy|method)", query, re.IGNORECASE):
            return False, "I can only discuss standard, widely-accepted technical analysis methodologies. Custom or personal frameworks cannot be validated or endorsed. Would you like to learn about standard RSI, MACD, or Moving Average strategies instead?"
        
        # Check for prediction requests
        if re.search(r"(will|should|predict|guarantee).*(go up|go down|price|reach)", query, re.IGNORECASE):
            return False, "I cannot predict future prices or guarantee any returns. Technical analysis studies historical patterns, not future outcomes. I can help you understand what historical data shows and how to interpret indicators."
        
        # Check mode transition
        is_safe, warning = FrameworkValidator.check_hypothetical_boundary(query, mode)
        if not is_safe:
            return False, warning
        
        # Check if strict mode needed
        if conversation_state.should_enforce_strict_mode():
            # Add strict mode indicator
            return True, f"[STRICT EDUCATIONAL MODE] {query}"
        
        return True, query
    
    @staticmethod
    def build_enhanced_system_prompt(base_prompt: str, conversation_state: ConversationStateTracker) -> str:
        """Build enhanced system prompt with lightweight safety guardrails"""
        
        # Light guardrails — don't overwhelm the base conversational prompt
        safety_addendum = """
## Quick Guardrails
- Use standard technical analysis definitions (e.g. RSI > 70 = overbought, not "momentum")
- If a user has a wrong definition, gently correct it in a friendly way — don't lecture
- Support/Resistance are zones, not exact lines
- All price data is historical (31-day lag). Use past tense
- Never give buy/sell advice or price predictions
"""
        
        if conversation_state.should_enforce_strict_mode():
            safety_addendum += """
Note: This user has had repeated misconceptions. Be a bit more thorough with corrections, but stay friendly.
"""
        
        return f"{base_prompt}\n\n{safety_addendum}"


# Global conversation state storage (per-user)
conversation_states: Dict[str, ConversationStateTracker] = {}


def get_conversation_state(user_id: str) -> ConversationStateTracker:
    """Get or create conversation state for user"""
    if user_id not in conversation_states:
        conversation_states[user_id] = ConversationStateTracker()
    return conversation_states[user_id]


def clear_conversation_state(user_id: str):
    """Clear conversation state (call after logout or timeout)"""
    if user_id in conversation_states:
        del conversation_states[user_id]


def validate_chat_input(query: str, mode: str, user_id: str) -> Tuple[bool, str, Optional[Dict]]:
    """
    Main validation function for chat input
    Returns: (is_valid, processed_query_or_error, metadata)
    """
    # Get conversation state
    conv_state = get_conversation_state(user_id)
    conv_state.update(query, mode)
    
    # Pre-validate
    is_valid, result = ChatbotSafetyEnforcer.pre_validate_input(query, mode, conv_state)
    if not is_valid:
        return False, result, {"blocked": True, "reason": "pre_validation"}
    
    processed_query = result
    
    # Check for non-standard frameworks
    is_nonstandard, correction_info = FrameworkValidator.detect_nonstandard_framework(query)
    if is_nonstandard:
        conv_state.record_correction(correction_info["framework"])
        is_strict = conv_state.should_enforce_strict_mode()
        correction_prompt = FrameworkValidator.build_correction_prompt(
            query, correction_info, is_strict
        )
        return True, correction_prompt, {
            "correction_needed": True,
            "framework": correction_info,
            "is_strict": is_strict
        }
    
    # Check mode transition
    mode_history = [m["mode"] for m in conv_state.mode_history]
    is_transition_valid, transition_warning = FrameworkValidator.validate_mode_transition(
        mode, mode_history, conv_state.suspicious_score
    )
    if not is_transition_valid:
        return True, f"[MODE TRANSITION WARNING] {processed_query}\n\nContext: {transition_warning}", {
            "mode_transition_warning": True,
            "warning": transition_warning
        }
    
    return True, processed_query, {
        "suspicious_score": conv_state.suspicious_score,
        "corrections_made": conv_state.correction_count
    }
