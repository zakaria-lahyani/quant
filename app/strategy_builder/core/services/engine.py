"""
Main strategy engine with dependency injection.
"""

from collections import deque
from typing import Dict, List
import pandas as pd

from app.strategy_builder.core.domain.protocols import StrategyLoaderInterface, EvaluatorFactory, Logger
from app.strategy_builder.core.domain.models import TradingStrategy
from app.strategy_builder.data.dtos import AllStrategiesEvaluationResult, StrategyEvaluationResult
from app.strategy_builder.core.services.executor import create_strategy_executor


class StrategyEngine:
    """Main strategy engine that orchestrates strategy evaluation."""
    
    def __init__(
        self,
        strategy_loader: StrategyLoaderInterface,
        evaluator_factory: EvaluatorFactory,
        logger: Logger
    ):
        """
        Initialize strategy engine with dependencies.
        
        Args:
            strategy_loader: Strategy loader instance
            evaluator_factory: Factory for creating evaluators
            logger: Logger instance
        """
        self.strategy_loader = strategy_loader
        self.evaluator_factory = evaluator_factory
        self.logger = logger
    
    def evaluate(self, recent_rows: Dict[str, deque[pd.Series]]) -> AllStrategiesEvaluationResult:
        """
        Evaluate all loaded strategies against market data.
        
        Args:
            recent_rows: Market data by timeframe
            
        Returns:
            Evaluation results for all strategies
        """
        self.logger.info("Starting strategy evaluation")
        
        try:
            strategies = self.strategy_loader.load_strategies()
            results: Dict[str, StrategyEvaluationResult] = {}
            
            for name, strategy in strategies.items():
                try:
                    self.logger.info(f"Evaluating strategy: {name}")
                    
                    # Create executor for this strategy
                    executor = create_strategy_executor(
                        strategy,
                        recent_rows,
                        self.evaluator_factory
                    )
                    
                    # Validate data availability
                    if not executor.validate_data_availability():
                        self.logger.warning(
                            f"Strategy {name}: Required market data not available"
                        )
                        continue
                    
                    # Check if strategy is active
                    if not executor.is_strategy_active():
                        self.logger.info(f"Strategy {name}: Currently inactive")
                        continue
                    
                    # Evaluate entry and exit signals
                    entry_signals = executor.check_entry()
                    exit_signals = executor.check_exit()
                    
                    results[name] = StrategyEvaluationResult(
                        strategy_name=name,
                        entry=entry_signals,
                        exit=exit_signals
                    )
                    
                    self.logger.info(
                        f"Strategy {name} evaluated - "
                        f"Entry: long={entry_signals.long}, short={entry_signals.short} | "
                        f"Exit: long={exit_signals.long}, short={exit_signals.short}"
                    )
                    
                except Exception as e:
                    self.logger.error(f"Failed to evaluate strategy {name}: {e}")
                    # Continue with other strategies instead of failing completely
                    continue
            
            self.logger.info(f"Strategy evaluation completed. Evaluated {len(results)} strategies")
            return AllStrategiesEvaluationResult(strategies=results)
            
        except Exception as e:
            self.logger.error(f"Strategy evaluation failed: {e}")
            raise
    
    def evaluate_single_strategy(
        self,
        strategy_name: str,
        recent_rows: Dict[str, deque[pd.Series]]
    ) -> StrategyEvaluationResult:
        """
        Evaluate a single strategy by name.
        
        Args:
            strategy_name: Name of strategy to evaluate
            recent_rows: Market data by timeframe
            
        Returns:
            Evaluation result for the strategy
            
        Raises:
            KeyError: If strategy is not found
        """
        self.logger.info(f"Evaluating single strategy: {strategy_name}")
        
        try:
            strategy = self.strategy_loader.get_strategy(strategy_name)
            
            executor = create_strategy_executor(
                strategy,
                recent_rows,
                self.evaluator_factory
            )
            
            # Validate data availability
            if not executor.validate_data_availability():
                raise ValueError(f"Required market data not available for strategy {strategy_name}")
            
            # Check if strategy is active
            if not executor.is_strategy_active():
                self.logger.warning(f"Strategy {strategy_name} is currently inactive")
            
            # Evaluate signals
            entry_signals = executor.check_entry()
            exit_signals = executor.check_exit()
            
            result = StrategyEvaluationResult(
                strategy_name=strategy_name,
                entry=entry_signals,
                exit=exit_signals
            )
            
            self.logger.info(f"Single strategy evaluation completed for {strategy_name}")
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to evaluate strategy {strategy_name}: {e}")
            raise
    
    def list_available_strategies(self) -> List[str]:
        """
        Get list of all available strategy names.
        
        Returns:
            List of strategy names
        """
        return self.strategy_loader.list_strategy_names()
    
    def get_strategy_info(self, strategy_name: str) -> TradingStrategy:
        """
        Get detailed information about a specific strategy.
        
        Args:
            strategy_name: Name of strategy
            
        Returns:
            TradingStrategy instance
            
        Raises:
            KeyError: If strategy is not found
        """
        return self.strategy_loader.get_strategy(strategy_name)
    
    def reload_strategies(self) -> None:
        """Force reload of all strategies from configuration files."""
        self.logger.info("Reloading strategies")
        self.strategy_loader.reload_strategies()
        self.logger.info("Strategies reloaded successfully")


def create_strategy_engine(
    strategy_loader: StrategyLoaderInterface,
    evaluator_factory: EvaluatorFactory,
    logger: Logger
) -> StrategyEngine:
    """
    Factory function to create strategy engine.
    
    Args:
        strategy_loader: Strategy loader instance
        evaluator_factory: Evaluator factory instance
        logger: Logger instance
        
    Returns:
        Strategy engine instance
    """
    return StrategyEngine(strategy_loader, evaluator_factory, logger)