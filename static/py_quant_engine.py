import json
import math
import numpy as np

def run_advanced_simulation(config_dict):
    """
    Runs the advanced quantitative Monte Carlo simulation.
    This function is executed by Pyodide in the user's browser, offloading the heavy math.
    """
    try:
        # 1. Parse Parameters
        num_simulations = int(config_dict.get('num_simulations', 1000))
        steps = int(config_dict.get('steps', 252)) # Days
        initial_capital = float(config_dict.get('initial_capital', 100000.0))
        
        # Drift / Vol
        mu = float(config_dict.get('mu', 0.05))
        vol = float(config_dict.get('vol', 0.20))
        
        # Heston
        use_heston = bool(config_dict.get('use_heston', False))
        kappa = float(config_dict.get('kappa', 2.0))
        theta = float(config_dict.get('theta', 0.04))
        xi = float(config_dict.get('xi', 0.1))
        rho = float(config_dict.get('rho', -0.5))
        
        # Jumps
        use_jumps = bool(config_dict.get('use_jumps', False))
        lambda_j = float(config_dict.get('lambda_j', 0.5))
        mu_j = float(config_dict.get('mu_j', -0.05))
        sigma_j = float(config_dict.get('sigma_j', 0.1))
        
        # Regimes
        use_regimes = bool(config_dict.get('use_regimes', False))
        p_bull_to_bear = float(config_dict.get('p_bull_to_bear', 0.1))
        p_bear_to_bull = float(config_dict.get('p_bear_to_bull', 0.3))
        mu_bear = float(config_dict.get('mu_bear', -0.10))
        vol_bear = float(config_dict.get('vol_bear', 0.40))
        
        # Set seed if provided
        seed = int(config_dict.get('seed', 0))
        if seed == 0:
            seed = np.random.randint(1, 4294967295)
        rng = np.random.default_rng(seed)
        
        # 2. Prepare arrays for paths
        dt = 1.0 / 252.0
        sqrt_dt = math.sqrt(dt)
        
        # We will track final returns to calculate percentiles and CVaR
        final_returns = np.zeros(num_simulations)
        max_drawdowns = np.zeros(num_simulations)
        
        # We also want to store a few sample paths for the UI Fan Chart (say 50 paths)
        num_samples = min(50, num_simulations)
        sample_paths = np.zeros((num_samples, steps + 1))
        
        # Calculate quantiles on the fly (store cross-sectional states at each time step)
        # To save memory, we don't store ALL paths, just the current step
        current_prices = np.full(num_simulations, initial_capital)
        current_vars = np.full(num_simulations, vol * vol)
        current_regimes = np.zeros(num_simulations, dtype=int) # 0 = Bull, 1 = Bear
        
        peak_prices = current_prices.copy()
        
        # Cross-sectional tracking
        percentiles_over_time = {
            'p5': np.zeros(steps + 1),
            'p25': np.zeros(steps + 1),
            'p50': np.zeros(steps + 1),
            'p75': np.zeros(steps + 1),
            'p95': np.zeros(steps + 1)
        }
        
        def save_step_metrics(t, prices):
            if num_simulations >= 20: 
                pcts = np.percentile(prices, [5, 25, 50, 75, 95])
                percentiles_over_time['p5'][t] = pcts[0]
                percentiles_over_time['p25'][t] = pcts[1]
                percentiles_over_time['p50'][t] = pcts[2]
                percentiles_over_time['p75'][t] = pcts[3]
                percentiles_over_time['p95'][t] = pcts[4]
            else:
                # Fallback purely for edge case mini tests
                for k in percentiles_over_time:
                    percentiles_over_time[k][t] = np.mean(prices)
                    
            if t == 0:
                sample_paths[:, 0] = prices[:num_samples]
            else:
                sample_paths[:, t] = prices[:num_samples]
                
        # Time 0
        save_step_metrics(0, current_prices)
        
        # 3. Time Stepping Loop
        for t in range(1, steps + 1):
            
            # Arrays for drift and vol this step
            mu_t = np.full(num_simulations, mu)
            vol_t = np.full(num_simulations, vol)
            
            # Regime switching
            if use_regimes:
                # Rolls for switching
                rolls = rng.random(num_simulations)
                
                # Bull -> Bear
                switch_to_bear = (current_regimes == 0) & (rolls < p_bull_to_bear)
                # Bear -> Bull
                switch_to_bull = (current_regimes == 1) & (rolls < p_bear_to_bull)
                
                current_regimes[switch_to_bear] = 1
                current_regimes[switch_to_bull] = 0
                
                bull_mask = (current_regimes == 0)
                bear_mask = (current_regimes == 1)
                
                mu_t[bear_mask] = mu_bear
                vol_t[bear_mask] = vol_bear
                
            # Random normals
            Z1 = rng.standard_normal(num_simulations)
            
            actual_vol = vol_t
            
            # Heston Stochastic Vol
            if use_heston:
                Z2 = rng.standard_normal(num_simulations)
                Z_v = rho * Z1 + math.sqrt(1.0 - rho*rho) * Z2
                
                # Discretized CIR for variance
                dv = kappa * (theta - current_vars) * dt + xi * np.sqrt(np.maximum(0, current_vars)) * sqrt_dt * Z_v
                current_vars = np.maximum(0.0, current_vars + dv)
                actual_vol = np.sqrt(current_vars)
                
            # Base Geometric Brownian Motion
            drift = (mu_t - 0.5 * actual_vol**2) * dt
            diffusion = actual_vol * sqrt_dt * Z1
            
            jump_magnitude = np.zeros(num_simulations)
            
            # Poisson Jumps
            if use_jumps:
                num_jumps = rng.poisson(lambda_j * dt, num_simulations)
                # For each path that has jumps, sample the magnitudes
                # Using a loop is slow for numpy, so we handle max jumps
                max_jumps_in_step = np.max(num_jumps)
                if max_jumps_in_step > 0:
                    jump_samples = rng.normal(mu_j, sigma_j, size=(num_simulations, max_jumps_in_step))
                    mask = np.arange(max_jumps_in_step) < num_jumps[:, None]
                    jump_magnitude = np.sum(jump_samples * mask, axis=1)
                    
            # Update Price
            current_prices = current_prices * np.exp(drift + diffusion + jump_magnitude)
            
            # Track drawdown (peak so far)
            peak_prices = np.maximum(peak_prices, current_prices)
            current_drawdown = (peak_prices - current_prices) / peak_prices * 100.0
            max_drawdowns = np.maximum(max_drawdowns, current_drawdown)
            
            # Save step for fan charts
            save_step_metrics(t, current_prices)
            
        # 4. Final Aggregations
        final_returns = (current_prices - initial_capital) / initial_capital * 100.0
        
        ret_percentiles = np.percentile(final_returns, [5, 25, 50, 75, 95])
        
        # CVaR (Expected Shortfall)
        var_95 = ret_percentiles[0]
        cvar_95 = np.mean(final_returns[final_returns <= var_95]) if np.any(final_returns <= var_95) else var_95
        
        prob_ruin = np.mean(final_returns <= -50.0) * 100.0
        
        # Create histogram for density plot
        hist, bin_edges = np.histogram(final_returns, bins=30)
        
        result = {
            "metadata": {
                "num_simulations": num_simulations,
                "seed_used": seed,
                "steps": steps,
            },
            "statistics": {
                "mean_return": float(np.mean(final_returns)),
                "median_return": float(ret_percentiles[2]),
                "mean_max_drawdown": float(np.mean(max_drawdowns)),
                "prob_ruin": float(prob_ruin),
                "var_95": float(var_95),
                "cvar_95": float(cvar_95)
            },
            "percentiles": {
                "p5": float(ret_percentiles[0]),
                "p25": float(ret_percentiles[1]),
                "p50": float(ret_percentiles[2]),
                "p75": float(ret_percentiles[3]),
                "p95": float(ret_percentiles[4])
            },
            "fan_chart": {
                "p5": percentiles_over_time['p5'].tolist(),
                "p25": percentiles_over_time['p25'].tolist(),
                "p50": percentiles_over_time['p50'].tolist(),
                "p75": percentiles_over_time['p75'].tolist(),
                "p95": percentiles_over_time['p95'].tolist()
            },
            "sample_paths": sample_paths.tolist(),
            "distribution": {
                "counts": hist.tolist(),
                "bins": bin_edges.tolist()
            }
        }
        
        return json.dumps(result)
        
    except Exception as e:
        import traceback
        return json.dumps({"error": str(e), "traceback": traceback.format_exc()})
