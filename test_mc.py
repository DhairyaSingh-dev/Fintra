#!/usr/bin/env python3
"""
Test script for Monte Carlo simulation engine
"""
import sys

# Suppress numpy warnings
import warnings

import numpy as np

warnings.filterwarnings('ignore')

def test_monte_carlo_engine():
    """Test the Monte Carlo engine with sample data"""
    print("Testing Monte Carlo Simulation Engine")
    print("=" * 50)
    
    try:
        from mc_engine import MonteCarloEngine, SimulationConfig

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
        
        print(f"Sample trades: {len(sample_trades)}")
        print(f"Sample prices: {len(sample_prices)} days")
        
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
        
        print(f"\nRunning {config.num_simulations} simulations...")
        
        # Run analysis
        analysis = engine.run_analysis(config)
        
        # Calculate p-values with original metrics
        original_return = 25.0  # Assume 25% return
        original_sharpe = 1.5
        
        analysis = engine.calculate_p_values(analysis, original_return, original_sharpe)
        
        print("\n[OK] Simulation complete!")
        print(f"   Seed used: {analysis.seed_used}")
        print(f"   Total trials: {analysis.num_trials}")
        
        # Display results
        print("\nResults Summary:")
        print(f"   Mean Return: {analysis.mean_return:.2f}%")
        print(f"   Median (P50): {analysis.percentile_50:.2f}%")
        print(f"   5th Percentile: {analysis.percentile_5:.2f}%")
        print(f"   95th Percentile: {analysis.percentile_95:.2f}%")
        
        print("\nOriginal Strategy vs Random:")
        print(f"   Your Return: {original_return:.2f}%")
        print(f"   P-Value: {analysis.p_value_strategy_vs_random:.2f}%")
        print(f"   Risk Rating: {analysis.risk_rating}")
        
        print("\nRisk Metrics:")
        print(f"   VaR (95%): {analysis.var_95:.2f}%")
        print(f"   CVaR (95%): {analysis.cvar_95:.2f}%")
        print(f"   Probability of Ruin: {analysis.prob_ruin:.2f}%")
        
        print("\n" + "=" * 50)
        print("[OK] All tests passed!")
        return True
        
    except Exception as e:
        print(f"\n[ERROR] Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    # Run tests
    success = test_monte_carlo_engine()
    sys.exit(0 if success else 1)
