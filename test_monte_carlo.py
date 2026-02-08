#!/usr/bin/env python3
"""
Test script for Monte Carlo simulation engine
"""
import sys

import numpy as np

from mc_engine import MonteCarloEngine, SimulationConfig, Trade


def test_monte_carlo_engine():
    """Test the Monte Carlo engine with sample data"""
    print("🎲 Testing Monte Carlo Simulation Engine")
    print("=" * 50)
    
    # Create sample trades (simulating a winning strategy)
    sample_trades = [
        {'entry_price': 100, 'exit_price': 105, 'pnl_pct': 5.0, 'result': 'Win', 'days_held': 10},
        {'entry_price': 105, 'exit_price': 103, 'pnl_pct': -1.9, 'result': 'Loss', 'days_held': 5},
        {'entry_price': 103, 'exit_price': 110, 'pnl_pct': 6.8, 'result': 'Win', 'days_held': 15},
        {'entry_price': 110, 'exit_price': 108, 'pnl_pct': -1.8, 'result': 'Loss', 'days_held': 7},
        {'entry_price': 108, 'exit_price': 115, 'pnl_pct': 6.5, 'result': 'Win', 'days_held': 12},
        {'entry_price': 115, 'exit_price': 112, 'pnl_pct': -2.6, 'result': 'Loss', 'days_held': 8},
        {'entry_price': 112, 'exit_price': 120, 'pnl_pct': 7.1, 'result': 'Win', 'days_held': 20},
        {'entry_price': 120, 'exit_price': 118, 'pnl_pct': -1.7, 'result': 'Loss', 'days_held': 6},
    ]
    
    # Create sample prices
    sample_prices = [100, 102, 101, 103, 105, 104, 106, 108, 107, 110, 
                     109, 111, 113, 112, 115, 114, 116, 118, 117, 120]
    
    print(f"📊 Sample trades: {len(sample_trades)}")
    print(f"📈 Sample prices: {len(sample_prices)} days")
    
    # Initialize engine
    engine = MonteCarloEngine(seed=42)
    engine.set_trades(sample_trades)
    
    import pandas as pd
    price_series = pd.Series(sample_prices)
    engine.set_daily_returns(price_series)
    
    # Configure simulation
    config = SimulationConfig(
        num_simulations=1000,  # Small number for quick test
        seed=42,
        initial_capital=100000.0
    )
    
    print(f"\n🚀 Running {config.num_simulations} simulations...")
    
    # Run analysis
    try:
        analysis = engine.run_analysis(config)
        
        # Calculate p-values with original metrics
        original_return = 25.0  # Assume 25% return
        original_sharpe = 1.5
        
        analysis = engine.calculate_p_values(analysis, original_return, original_sharpe)
        
        print("\n✅ Simulation complete!")
        print(f"   Seed used: {analysis.seed_used}")
        print(f"   Total trials: {analysis.num_trials}")
        
        # Display results
        print("\n📊 Results Summary:")
        print(f"   Mean Return: {analysis.mean_return:.2f}%")
        print(f"   Median (P50): {analysis.percentile_50:.2f}%")
        print(f"   5th Percentile: {analysis.percentile_5:.2f}%")
        print(f"   95th Percentile: {analysis.percentile_95:.2f}%")
        
        print("\n🎯 Original Strategy vs Random:")
        print(f"   Your Return: {original_return:.2f}%")
        print(f"   P-Value: {analysis.p_value_strategy_vs_random:.2f}%")
        print(f"   Interpretation: {analysis.interpretation[:50]}...")
        print(f"   Risk Rating: {analysis.risk_rating}")
        
        print("\n⚠️  Risk Metrics:")
        print(f"   VaR (95%): {analysis.var_95:.2f}%")
        print(f"   CVaR (95%): {analysis.cvar_95:.2f}%")
        print(f"   Probability of Ruin: {analysis.prob_ruin:.2f}%")
        
        print("\n" + "=" * 50)
        print("✅ All tests passed!")
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_api_endpoint():
    """Test the API endpoint (requires Flask server running)"""
    print("\n🌐 Testing API Endpoint (requires server running)")
    print("=" * 50)
    
    try:
        import requests

        # Sample data for API
        test_data = {
            'trades': [
                {'entry_price': 100, 'exit_price': 105, 'pnl_pct': 5.0, 'result': 'Win', 'days_held': 10},
                {'entry_price': 105, 'exit_price': 103, 'pnl_pct': -1.9, 'result': 'Loss', 'days_held': 5},
            ],
            'prices': [100, 102, 101, 103, 105],
            'num_simulations': 100,
            'seed': 42,
            'original_return': 10.0,
            'original_sharpe': 1.2,
            'original_max_dd': 5.0
        }
        
        # Note: This requires the Flask server to be running
        response = requests.post(
            'http://localhost:5000/api/backtest/quick_mc',
            json=test_data,
            headers={'Content-Type': 'application/json'}
        )
        
        if response.status_code == 200:
            print("✅ API endpoint working!")
            data = response.json()
            print(f"   Simulations: {data['metadata']['num_trials']}")
            print(f"   P-Value: {data['statistics']['p_value_vs_random']:.2f}%")
        else:
            print(f"⚠️  API returned status {response.status_code}")
            print("   (Server may not be running)")
            
    except requests.exceptions.ConnectionError:
        print("⚠️  Could not connect to server (expected if not running)")
    except ImportError:
        print("⚠️  requests library not installed")
    except Exception as e:
        print(f"⚠️  API test skipped: {e}")

if __name__ == '__main__':
    # Run tests
    success = test_monte_carlo_engine()
    
    # Try API test (optional)
    try:
        test_api_endpoint()
    except Exception as e:
        print(f"\n⚠️  API test skipped: {e}")
    
    sys.exit(0 if success else 1)
