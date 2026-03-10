"""
Monte Carlo Simulation Engine
High-performance C++-powered simulation bridge for Python
"""
import hashlib
import json
import logging
import pickle
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class Trade:
    """Trade data structure matching C++ Trade struct"""
    entry_price: float
    exit_price: float
    days_held: int
    pnl_pct: float
    is_win: bool
    
    def to_dict(self) -> Dict:
        return {
            'entry_price': self.entry_price,
            'exit_price': self.exit_price,
            'days_held': self.days_held,
            'pnl_pct': self.pnl_pct,
            'is_win': self.is_win
        }


@dataclass
class SimulationConfig:
    """Configuration for Monte Carlo simulations"""
    num_simulations: int = 10000
    seed: int = 0
    initial_capital: float = 100000.0
    risk_per_trade: float = 0.02
    atr_multiplier: float = 3.0
    tax_rate: float = 0.002
    
    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class SimulationResult:
    """Result from a single simulation"""
    final_value: float
    total_return_pct: float
    max_drawdown_pct: float
    num_trades: int
    win_rate: float
    sharpe_ratio: float
    equity_curve: Optional[List[float]] = None
    
    def to_dict(self) -> Dict:
        result = {
            'final_value': self.final_value,
            'total_return_pct': self.total_return_pct,
            'max_drawdown_pct': self.max_drawdown_pct,
            'num_trades': self.num_trades,
            'win_rate': self.win_rate,
            'sharpe_ratio': self.sharpe_ratio
        }
        if self.equity_curve is not None:
            result['equity_curve'] = self.equity_curve[:100]  # Limit for performance
        return result


@dataclass
class MonteCarloAnalysis:
    """Complete Monte Carlo analysis results"""
    # Simulations
    simulations: List[SimulationResult]
    
    # Statistical metrics
    p_value_strategy_vs_random: float
    p_value_strategy_vs_bootstrap: float
    
    # Percentiles
    percentile_5: float
    percentile_25: float
    percentile_50: float
    percentile_75: float
    percentile_95: float
    
    # Confidence intervals
    ci_lower_95: float
    ci_upper_95: float
    
    # Original strategy metrics
    original_return: float
    original_sharpe: float
    original_max_dd: float
    
    # Distribution histogram (20 bins)
    return_distribution: List[int]
    distribution_min: float
    distribution_max: float
    
    # Metadata
    seed_used: int
    num_trials: int
    
    # Summary statistics
    mean_return: float
    mean_sharpe: float
    mean_max_drawdown: float
    
    # Interpretation
    interpretation: str
    risk_rating: str  # Green/Amber/Red
    
    # Risk metrics
    var_95: float  # Value at Risk
    cvar_95: float  # Conditional VaR
    prob_ruin: float  # Probability of >50% loss
    
    def to_dict(self) -> Dict:
        return {
            'simulations': [s.to_dict() for s in self.simulations[:100]],  # Limit sample size
            'statistics': {
                'p_value_vs_random': self.p_value_strategy_vs_random,
                'p_value_vs_bootstrap': self.p_value_strategy_vs_bootstrap,
                'percentiles': {
                    'p5': self.percentile_5,
                    'p25': self.percentile_25,
                    'p50': self.percentile_50,
                    'p75': self.percentile_75,
                    'p95': self.percentile_95
                },
                'confidence_interval_95': {
                    'lower': self.ci_lower_95,
                    'upper': self.ci_upper_95
                }
            },
            'original_strategy': {
                'return_pct': self.original_return,
                'sharpe_ratio': self.original_sharpe,
                'max_drawdown_pct': self.original_max_dd
            },
            'distribution': {
                'histogram': self.return_distribution,
                'min': self.distribution_min,
                'max': self.distribution_max
            },
            'metadata': {
                'seed_used': self.seed_used,
                'num_trials': self.num_trials,
                'timestamp': datetime.now().isoformat()
            },
            'summary': {
                'mean_return': self.mean_return,
                'mean_sharpe': self.mean_sharpe,
                'mean_max_drawdown': self.mean_max_drawdown,
                'interpretation': self.interpretation,
                'risk_rating': self.risk_rating
            },
            'risk_metrics': {
                'var_95': self.var_95,
                'cvar_95': self.cvar_95,
                'probability_of_ruin': self.prob_ruin
            }
        }


class MonteCarloEngine:
    """
    High-performance Monte Carlo simulation engine
    Uses NumPy for vectorized operations
    """
    
    def __init__(self, seed: Optional[int] = None):
        """
        Initialize the Monte Carlo engine
        
        Args:
            seed: Random seed for reproducibility (0 = auto-generate)
        """
        if seed is None or seed == 0:
            seed = np.random.randint(0, 2**32 - 1)
        self.seed = seed
        self.rng = np.random.RandomState(seed)
        self.trades: List[Trade] = []
        self.daily_returns: np.ndarray = np.array([])
        
    def set_trades(self, trades: List[Dict]):
        """Set trade data from backtest results"""
        self.trades = [
            Trade(
                entry_price=t['entry_price'],
                exit_price=t['exit_price'],
                days_held=t.get('days_held', 0),
                pnl_pct=t['pnl_pct'],
                is_win=t.get('result', 'Loss') == 'Win'
            )
            for t in trades
        ]
        
    def set_daily_returns(self, prices: pd.Series):
        """Set daily returns from price series"""
        self.daily_returns = prices.pct_change().dropna().values
        
    def _calculate_sharpe(self, equity_curve: np.ndarray) -> float:
        """Calculate annualized Sharpe ratio"""
        if len(equity_curve) < 2:
            return 0.0
        
        returns = np.diff(equity_curve) / equity_curve[:-1]
        if len(returns) == 0 or np.std(returns) == 0:
            return 0.0
            
        return (np.mean(returns) / np.std(returns)) * np.sqrt(252)
    
    def _calculate_max_drawdown(self, equity_curve: np.ndarray) -> float:
        """Calculate maximum drawdown percentage"""
        if len(equity_curve) == 0:
            return 0.0
            
        peak = np.maximum.accumulate(equity_curve)
        drawdown = (peak - equity_curve) / peak
        return np.max(drawdown) * 100
    
    def run_position_shuffle(self, num_simulations: int) -> List[SimulationResult]:
        """
        Run position shuffle simulations
        Randomizes the order of trades while keeping the same P&L distribution
        """
        if not self.trades:
            return []
            
        trade_pnls = np.array([t.pnl_pct / 100.0 for t in self.trades])
        results = []
        
        for _ in range(num_simulations):
            # Shuffle trade order
            shuffled_pnls = self.rng.permutation(trade_pnls)
            
            # Simulate equity curve
            equity_curve = np.zeros(len(shuffled_pnls) + 1)
            equity_curve[0] = 100000.0
            
            for i, pnl in enumerate(shuffled_pnls):
                equity_curve[i + 1] = equity_curve[i] * (1 + pnl)
            
            # Calculate metrics
            wins = np.sum(shuffled_pnls > 0)
            
            result = SimulationResult(
                final_value=float(equity_curve[-1]),
                total_return_pct=float((equity_curve[-1] - 100000.0) / 100000.0 * 100),
                max_drawdown_pct=float(self._calculate_max_drawdown(equity_curve)),
                num_trades=len(shuffled_pnls),
                win_rate=float(wins / len(shuffled_pnls) * 100),
                sharpe_ratio=float(self._calculate_sharpe(equity_curve)),
                equity_curve=equity_curve.tolist()
            )
            results.append(result)
            
        return results
    
    def run_return_permutation(self, num_simulations: int) -> List[SimulationResult]:
        """
        Run return permutation simulations
        Randomizes the order of daily returns
        """
        if len(self.daily_returns) == 0:
            return []
            
        num_days = len(self.daily_returns)
        results = []
        
        for _ in range(num_simulations):
            # Permute daily returns
            permuted_returns = self.rng.permutation(self.daily_returns)
            
            # Simulate equity curve
            equity_curve = np.zeros(num_days + 1)
            equity_curve[0] = 100000.0
            
            for i, ret in enumerate(permuted_returns):
                equity_curve[i + 1] = equity_curve[i] * (1 + ret)
            
            result = SimulationResult(
                final_value=float(equity_curve[-1]),
                total_return_pct=float((equity_curve[-1] - 100000.0) / 100000.0 * 100),
                max_drawdown_pct=float(self._calculate_max_drawdown(equity_curve)),
                num_trades=num_days // 20,  # Approximate
                win_rate=50.0,  # Random walk
                sharpe_ratio=float(self._calculate_sharpe(equity_curve)),
                equity_curve=equity_curve[::max(1, num_days // 100)].tolist()  # Sample for efficiency
            )
            results.append(result)
            
        return results
    
    def run_bootstrap(self, num_simulations: int) -> List[SimulationResult]:
        """
        Run bootstrap simulations
        Samples trades with replacement
        """
        if not self.trades:
            return []
            
        trade_pnls = np.array([t.pnl_pct / 100.0 for t in self.trades])
        n_trades = len(trade_pnls)
        results = []
        
        for _ in range(num_simulations):
            # Bootstrap sample with replacement
            bootstrapped_pnls = self.rng.choice(trade_pnls, size=n_trades, replace=True)
            
            # Simulate equity curve
            equity_curve = np.zeros(n_trades + 1)
            equity_curve[0] = 100000.0
            
            for i, pnl in enumerate(bootstrapped_pnls):
                equity_curve[i + 1] = equity_curve[i] * (1 + pnl)
            
            # Calculate metrics
            wins = np.sum(bootstrapped_pnls > 0)
            
            result = SimulationResult(
                final_value=float(equity_curve[-1]),
                total_return_pct=float((equity_curve[-1] - 100000.0) / 100000.0 * 100),
                max_drawdown_pct=float(self._calculate_max_drawdown(equity_curve)),
                num_trades=n_trades,
                win_rate=float(wins / n_trades * 100),
                sharpe_ratio=float(self._calculate_sharpe(equity_curve)),
                equity_curve=equity_curve.tolist()
            )
            results.append(result)
            
        return results
    
    def run_analysis(self, config: SimulationConfig) -> MonteCarloAnalysis:
        """
        Run complete Monte Carlo analysis with all three methods
        """
        logger.info(f"Starting Monte Carlo analysis: {config.num_simulations} simulations, seed={self.seed}")
        
        # Divide simulations between methods
        n_per_method = config.num_simulations // 3
        
        # Run all three simulation types
        shuffle_results = self.run_position_shuffle(n_per_method)
        perm_results = self.run_return_permutation(n_per_method)
        bootstrap_results = self.run_bootstrap(n_per_method)
        
        # Combine results
        all_simulations = shuffle_results + perm_results + bootstrap_results
        
        # Extract return values for analysis
        returns = np.array([sim.total_return_pct for sim in all_simulations])
        
        # Calculate percentiles
        percentiles = np.percentile(returns, [5, 25, 50, 75, 95])
        
        # Build histogram (20 bins)
        hist, bin_edges = np.histogram(returns, bins=20)
        
        # Calculate confidence intervals
        ci_lower = percentiles[0]  # 5th percentile
        ci_upper = percentiles[4]  # 95th percentile
        
        # Calculate means
        mean_return = np.mean(returns)
        mean_sharpe = np.mean([sim.sharpe_ratio for sim in all_simulations])
        mean_max_dd = np.mean([sim.max_drawdown_pct for sim in all_simulations])
        
        # Risk metrics
        var_95 = percentiles[0]  # 5th percentile = 95% VaR
        cvar_95 = np.mean(returns[returns <= var_95]) if np.any(returns <= var_95) else var_95
        prob_ruin = np.mean(returns < -50.0) * 100  # Probability of >50% loss
        
        # Determine interpretation
        # For now, use placeholder original metrics
        original_return = 0.0  # Will be set by caller
        original_sharpe = 0.0
        original_max_dd = 0.0
        
        # Create result
        analysis = MonteCarloAnalysis(
            simulations=all_simulations,
            p_value_strategy_vs_random=0.0,  # Calculated later
            p_value_strategy_vs_bootstrap=0.0,
            percentile_5=float(percentiles[0]),
            percentile_25=float(percentiles[1]),
            percentile_50=float(percentiles[2]),
            percentile_75=float(percentiles[3]),
            percentile_95=float(percentiles[4]),
            ci_lower_95=float(ci_lower),
            ci_upper_95=float(ci_upper),
            original_return=original_return,
            original_sharpe=original_sharpe,
            original_max_dd=original_max_dd,
            return_distribution=hist.tolist(),
            distribution_min=float(bin_edges[0]),
            distribution_max=float(bin_edges[-1]),
            seed_used=self.seed,
            num_trials=len(all_simulations),
            mean_return=float(mean_return),
            mean_sharpe=float(mean_sharpe),
            mean_max_drawdown=float(mean_max_dd),
            interpretation="",  # Set later
            risk_rating="",
            var_95=float(var_95),
            cvar_95=float(cvar_95),
            prob_ruin=float(prob_ruin)
        )
        
        logger.info(f"Monte Carlo analysis complete: {len(all_simulations)} simulations")
        return analysis
    
    def calculate_p_values(self, analysis: MonteCarloAnalysis, 
                          original_return: float,
                          original_sharpe: float) -> MonteCarloAnalysis:
        """Calculate p-values and update interpretation"""
        
        returns = np.array([sim.total_return_pct for sim in analysis.simulations])
        
        # Calculate p-value (what % of simulations beat the original strategy?)
        p_value = np.mean(returns >= original_return) * 100
        
        analysis.original_return = original_return
        analysis.original_sharpe = original_sharpe
        analysis.p_value_strategy_vs_random = float(p_value)
        analysis.p_value_strategy_vs_bootstrap = float(p_value)
        
        # Generate interpretation
        if original_return > analysis.percentile_95:
            analysis.interpretation = (
                "STRONG_SIGNAL: Strategy significantly outperforms random permutations (>95th percentile). "
                "Results are likely NOT due to luck."
            )
            analysis.risk_rating = "GREEN"
        elif original_return > analysis.percentile_75:
            analysis.interpretation = (
                "MODERATE_SIGNAL: Strategy performs better than 75% of random permutations. "
                "Results suggest skill over luck."
            )
            analysis.risk_rating = "GREEN"
        elif original_return > analysis.percentile_50:
            analysis.interpretation = (
                "WEAK_SIGNAL: Strategy performs above median but not exceptionally. "
                "Results may have some skill component."
            )
            analysis.risk_rating = "AMBER"
        else:
            analysis.interpretation = (
                "NO_SIGNAL: Strategy does not outperform random permutations. "
                "Results likely due to luck."
            )
            analysis.risk_rating = "RED"
            
        return analysis


# Cache manager for simulation results
class SimulationCache:
    """Simple cache for Monte Carlo simulation results"""
    
    def __init__(self, cache_dir: str = "./cache"):
        self.cache_dir = cache_dir
        
    def _get_cache_key(self, trades: List[Dict], config: SimulationConfig) -> str:
        """Generate cache key from trades and config"""
        data = json.dumps({
            'trades': trades,
            'config': config.to_dict()
        }, sort_keys=True)
        return hashlib.md5(data.encode()).hexdigest()
    
    def get(self, trades: List[Dict], config: SimulationConfig) -> Optional[MonteCarloAnalysis]:
        """Retrieve cached result if available"""
        key = self._get_cache_key(trades, config)
        # In production, use Redis or file-based cache
        return None
    
    def set(self, trades: List[Dict], config: SimulationConfig, result: MonteCarloAnalysis):
        """Cache simulation result"""
        key = self._get_cache_key(trades, config)
        # In production, store in Redis or file
        pass


# Global cache instance
simulation_cache = SimulationCache()
