"""
Unit tests for authentication module
"""
from datetime import datetime, timedelta, timezone

import jwt as pyjwt
import pytest

from auth import generate_jwt_token, verify_jwt_token


class TestJWTGeneration:
    """Tests for JWT token generation"""
    
    def test_generate_access_token(self):
        """Test access token generation"""
        user_data = {
            'user_id': 'test_user_123',
            'email': 'test@example.com',
            'name': 'Test User'
        }
        secret = 'test-secret-key'
        expires_in = '15m'
        
        token = generate_jwt_token(user_data, secret, expires_in)
        
        assert token is not None
        assert isinstance(token, str)
        
        # Decode without verification to check payload
        decoded = pyjwt.decode(token, options={'verify_signature': False})
        assert decoded['user_id'] == 'test_user_123'
        assert decoded['email'] == 'test@example.com'
        assert 'exp' in decoded
        assert 'iat' in decoded
    
    def test_generate_refresh_token(self):
        """Test refresh token generation"""
        user_data = {
            'user_id': 'test_user_456',
            'email': 'user@example.com',
            'name': 'Another User'
        }
        secret = 'test-refresh-secret'
        expires_in = '7d'
        
        token = generate_jwt_token(user_data, secret, expires_in)
        
        assert token is not None
        decoded = pyjwt.decode(token, options={'verify_signature': False})
        assert decoded['user_id'] == 'test_user_456'


class TestJWTVerification:
    """Tests for JWT token verification"""
    
    def test_verify_valid_token(self):
        """Test verification of valid token"""
        user_data = {'user_id': 'test_123', 'email': 'test@test.com', 'name': 'Test'}
        secret = 'test-secret'
        expires_in = '15m'
        
        token = generate_jwt_token(user_data, secret, expires_in)
        payload = verify_jwt_token(token, secret)
        
        assert payload is not None
        assert payload['user_id'] == 'test_123'
        assert payload['email'] == 'test@test.com'
    
    def test_verify_expired_token(self):
        """Test verification of expired token"""
        user_data = {'user_id': 'test_123', 'email': 'test@test.com', 'name': 'Test'}
        secret = 'test-secret'
        
        # Create a token that expired 1 hour ago
        payload = {
            'user_id': user_data['user_id'],
            'email': user_data['email'],
            'name': user_data['name'],
            'exp': datetime.now(timezone.utc) - timedelta(hours=1),
            'iat': datetime.now(timezone.utc) - timedelta(hours=2)
        }
        token = pyjwt.encode(payload, secret, algorithm='HS256')
        
        result = verify_jwt_token(token, secret)
        assert result is None
    
    def test_verify_invalid_signature(self):
        """Test verification with wrong secret"""
        user_data = {'user_id': 'test_123', 'email': 'test@test.com', 'name': 'Test'}
        secret = 'correct-secret'
        wrong_secret = 'wrong-secret'
        
        token = generate_jwt_token(user_data, secret, '15m')
        result = verify_jwt_token(token, wrong_secret)
        
        assert result is None
    
    def test_verify_malformed_token(self):
        """Test verification of malformed token"""
        result = verify_jwt_token('not.a.valid.token', 'secret')
        assert result is None
