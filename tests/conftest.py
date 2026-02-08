"""
Pytest configuration and fixtures for Fintra application tests
"""
import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app
from database import db
from models import Position, User


@pytest.fixture(scope='session')
def app():
    """Create application for testing"""
    # Set test environment variables
    os.environ['FLASK_ENV'] = 'testing'
    os.environ['DATABASE_URL'] = 'sqlite:///:memory:'
    os.environ['ACCESS_TOKEN_JWT_SECRET'] = 'test-access-secret-key-12345'
    os.environ['REFRESH_TOKEN_JWT_SECRET'] = 'test-refresh-secret-key-67890'
    os.environ['GOOGLE_CLIENT_ID'] = 'test-google-client-id'
    os.environ['GOOGLE_CLIENT_SECRET'] = 'test-google-client-secret'
    os.environ['CLIENT_ORIGIN'] = 'http://localhost:3000'
    
    app = create_app()
    app.config.update({
        'TESTING': True,
        'SQLALCHEMY_DATABASE_URI': 'sqlite:///:memory:',
        'WTF_CSRF_ENABLED': False,
        'SESSION_COOKIE_SECURE': False,
        'SESSION_COOKIE_SAMESITE': 'Lax'
    })
    
    with app.app_context():
        db.create_all()
        yield app
        db.session.remove()
        db.drop_all()


@pytest.fixture(scope='function')
def client(app):
    """Create test client"""
    return app.test_client()


@pytest.fixture(scope='function')
def runner(app):
    """Create test CLI runner"""
    return app.test_cli_runner()


@pytest.fixture(scope='function')
def db_session(app):
    """Create database session for tests"""
    with app.app_context():
        # Clear all tables before each test
        db.session.remove()
        db.drop_all()
        db.create_all()
        yield db.session
        db.session.rollback()


@pytest.fixture
def test_user(db_session):
    """Create a test user"""
    user = User(
        google_user_id='test_google_user_123',
        email='test@example.com',
        name='Test User',
        picture='https://example.com/avatar.jpg'
    )
    db_session.add(user)
    db_session.commit()
    return user


@pytest.fixture
def test_position(db_session, test_user):
    """Create a test portfolio position"""
    position = Position(
        user_id=test_user.id,
        symbol='RELIANCE.NS',
        quantity=10,
        entry_price=2500.00,
        entry_date=datetime.now(timezone.utc) - timedelta(days=60),
        notes='Test position for RELIANCE'
    )
    db_session.add(position)
    db_session.commit()
    return position


@pytest.fixture
def auth_headers(test_user):
    """Generate authentication headers for test user"""
    from auth import generate_jwt_token
    from config import Config
    
    user_data = {
        'user_id': test_user.google_user_id,
        'email': test_user.email,
        'name': test_user.name
    }
    
    access_token = generate_jwt_token(
        user_data, 
        Config.ACCESS_TOKEN_JWT_SECRET, 
        Config.ACCESS_TOKEN_EXPIRETIME
    )
    
    return {'Authorization': f'Bearer {access_token}'}


@pytest.fixture
def mock_stock_data():
    """Mock stock data for testing"""
    return [
        {
            'Date': '2024-01-01',
            'Open': 2500.0,
            'High': 2550.0,
            'Low': 2480.0,
            'Close': 2530.0,
            'Volume': 1000000
        },
        {
            'Date': '2024-01-02',
            'Open': 2530.0,
            'High': 2580.0,
            'Low': 2520.0,
            'Close': 2570.0,
            'Volume': 1200000
        },
        {
            'Date': '2024-01-03',
            'Open': 2570.0,
            'High': 2600.0,
            'Low': 2560.0,
            'Close': 2590.0,
            'Volume': 1100000
        }
    ]


@pytest.fixture
def mock_backtest_data():
    """Mock backtest request data"""
    return {
        'symbol': 'RELIANCE.NS',
        'strategy': 'rsi_macd',
        'initial_balance': 100000.0,
        'start_date': '2023-01-01',
        'end_date': '2023-12-31',
        'mode': 'beginner',
        'atr_multiplier': 3.0,
        'risk_per_trade': 0.02
    }
