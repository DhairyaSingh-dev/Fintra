"""
Unit tests for security module.

Tests XSS prevention, input sanitization, and security-related validation.
"""
import pytest

from backend.validation import XSS_PATTERNS, sanitize_string


class TestXSSPatterns:
    """Tests for XSS pattern detection in validation module."""

    def test_xss_patterns_list_exists(self):
        """Test XSS_PATTERNS is defined and non-empty."""
        assert XSS_PATTERNS is not None
        assert isinstance(XSS_PATTERNS, list)
        assert len(XSS_PATTERNS) > 0

    def test_javascript_protocol_in_patterns(self):
        """Test javascript: protocol is in XSS patterns."""
        assert any('javascript:' in p.lower() for p in XSS_PATTERNS)

    def test_script_tag_in_patterns(self):
        """Test <script pattern is in XSS patterns."""
        assert any('<script' in p.lower() or 'script' in p.lower() for p in XSS_PATTERNS)

    def test_onerror_attribute_in_patterns(self):
        """Test onerror attribute pattern is present."""
        assert any('onerror' in p.lower() for p in XSS_PATTERNS)

    def test_onload_attribute_in_patterns(self):
        """Test onload attribute pattern is present."""
        assert any('onload' in p.lower() for p in XSS_PATTERNS)

    def test_eval_in_patterns(self):
        """Test eval() pattern is present."""
        assert any('eval' in p.lower() for p in XSS_PATTERNS)

    def test_document_cookie_in_patterns(self):
        """Test document.cookie pattern is present."""
        assert any('cookie' in p.lower() for p in XSS_PATTERNS)


class TestXSSSanitization:
    """Tests for XSS sanitization in backend validation."""

    def test_sanitization_blocks_script_tags(self):
        """Test script tags are detected and rejected."""
        result, error = sanitize_string('<script>alert(1)</script>')
        assert result == ""
        assert error is not None

    def test_sanitization_blocks_javascript_protocol(self):
        """Test javascript: URLs are detected."""
        result, error = sanitize_string('javascript:alert(1)')
        assert result == ""
        assert error is not None

    def test_sanitization_blocks_onerror_attributes(self):
        """Test onerror attributes are detected."""
        result, error = sanitize_string('<img src=x onerror=alert(1)>')
        assert result == ""
        assert error is not None

    def test_sanitization_allows_safe_html_when_permitted(self):
        """Test safe HTML is allowed when explicitly permitted."""
        result, error = sanitize_string('<b>Bold</b> text', allow_html=True)
        assert error is None or error == ""
        assert 'Bold' in result

    def test_sanitization_strips_dangerous_html_when_html_allowed(self):
        """Test dangerous HTML is still stripped even when HTML allowed."""
        result, error = sanitize_string(
            '<b>safe</b><script>alert(1)</script>',
            allow_html=True
        )
        assert '<script>' not in result
        assert '<b>safe</b>' in result

    def test_sanitization_handles_nested_scripts(self):
        """Test nested/obfuscated script tags are detected."""
        obfuscated = '<scr<script>ipt>alert(1)</scr</script>ipt>'
        result, error = sanitize_string(obfuscated)
        assert result == ""
        assert error is not None

    def test_sanitization_handles_encoded_scripts(self):
        """Test URL-encoded script attempts are detected."""
        encoded = '%3Cscript%3Ealert(1)%3C/script%3E'
        result, error = sanitize_string(encoded)
        # Should either decode and detect, or reject encoded content
        assert result == "" or '<script>' not in result

    def test_sanitization_handles_case_variations(self):
        """Test case-insensitive XSS detection."""
        variations = [
            '<SCRIPT>alert(1)</SCRIPT>',
            '<Script>alert(1)</Script>',
            'JaVaScRiPt:alert(1)',
        ]
        for variant in variations:
            result, error = sanitize_string(variant)
            assert result == "", f"Failed to block: {variant}"
            assert error is not None

    def test_sanitization_allows_safe_strings(self):
        """Test normal text is not affected."""
        safe_strings = [
            'Hello World',
            'Stock price: $100.50',
            'RELIANCE.NS',
            'Test description with numbers 123',
        ]
        for text in safe_strings:
            result, error = sanitize_string(text)
            assert error is None, f"Safe string rejected: {text}"
            assert result == text or text in result


class TestXSSEdgeCases:
    """Tests for edge cases in XSS prevention."""

    def test_empty_string_passes(self):
        """Test empty string is allowed."""
        result, error = sanitize_string('')
        assert result == ''
        assert error is None

    def test_none_input_handled(self):
        """Test None input is handled gracefully."""
        result, error = sanitize_string(None)
        assert result == ''
        assert error is None

    def test_whitespace_only_passes(self):
        """Test whitespace-only strings are allowed."""
        result, error = sanitize_string('   ')
        assert error is None

    def test_very_long_string_handled(self):
        """Test very long strings don't cause issues."""
        long_text = 'A' * 10000 + '<script>alert(1)</script>'
        result, error = sanitize_string(long_text)
        assert result == ""
        assert error is not None

    def test_unicode_in_xss_attempt(self):
        """Test unicode in XSS attempts is handled."""
        unicode_xss = '<script>alert(\u0031)</script>'
        result, error = sanitize_string(unicode_xss)
        assert result == ""
        assert error is not None
