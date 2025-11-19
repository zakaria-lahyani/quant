"""
Multi-Timeframe Regime Manager
==============================

Manages regime detection across multiple timeframes with centralized warmup and streaming updates.
"""

import logging
from typing import Dict, Optional, List, Any
import pandas as pd
from collections import defaultdict

from app.regime.regime_detector import RegimeDetector
from app.regime.data_structure import BarData, RegimeSnapshot


class RegimeManager:
    """
    Orchestrates regime detection across multiple timeframes.
    
    This class provides:
    - Centralized initialization and warmup for all timeframes
    - Streaming updates with new bar data
    - Easy access to current regime states
    - Enrichment data for recent_rows
    """
    
    def __init__(self, 
                 warmup_bars: int = 500,
                 persist_n: int = 2,
                 transition_bars: int = 3,
                 bb_threshold_len: int = 200,
                 htf_rule: Optional[str] = None):
        """
        Initialize the RegimeManager.
        
        Args:
            warmup_bars: Number of bars needed for regime warmup
            persist_n: Number of bars to persist regime state
            transition_bars: Number of bars for transition detection
            bb_threshold_len: Length for Bollinger Bands threshold calculation
            htf_rule: Higher timeframe rule (optional)
        """
        self.warmup_bars = warmup_bars
        self.persist_n = persist_n
        self.transition_bars = transition_bars
        self.bb_threshold_len = bb_threshold_len
        self.htf_rule = htf_rule
        
        # Store detectors by timeframe
        self.detectors: Dict[str, RegimeDetector] = {}
        
        # Store latest regime states by timeframe
        self.latest_regimes: Dict[str, RegimeSnapshot] = {}
        
        # Track bar indices for each timeframe
        self.bar_counters: Dict[str, int] = defaultdict(int)
        
        # Setup logging
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def setup(self, timeframes: List[str], historicals: Dict[str, pd.DataFrame]) -> None:
        """
        Setup and warmup regime detectors for all timeframes.
        
        Args:
            timeframes: List of timeframe identifiers (e.g., ['5', '15', '30'])
            historicals: Dictionary mapping timeframe to historical DataFrame
        """
        self.logger.info(f"Setting up regime detectors for timeframes: {timeframes}")
        
        for tf in timeframes:
            # Create detector for this timeframe
            detector = RegimeDetector(
                warmup=self.warmup_bars,
                persist_n=self.persist_n,
                transition_bars=self.transition_bars,
                bb_threshold_len=self.bb_threshold_len,
                htf_rule=self.htf_rule
            )
            
            # Warmup with historical data if available
            if tf in historicals:
                df = historicals[tf]
                self._warmup_detector(detector, df, tf)
                self.bar_counters[tf] = len(df)
            else:
                self.logger.warning(f"No historical data found for timeframe {tf}")
                self.bar_counters[tf] = 0
            
            self.detectors[tf] = detector
            
            # Store latest regime if available
            if detector.history:
                self.latest_regimes[tf] = detector.history[-1]
                self.logger.info(
                    f"Timeframe {tf}: Initial regime={self.latest_regimes[tf].regime}, "
                    f"confidence={self.latest_regimes[tf].confidence:.2%}"
                )
    
    def update(self, timeframe: str, bar_data: pd.Series) -> Dict[str, Any]:
        """
        Update regime detector with new bar data.
        
        Args:
            timeframe: Timeframe identifier
            bar_data: New bar data as pandas Series
            
        Returns:
            Dictionary with regime information for enrichment
        """
        if timeframe not in self.detectors:
            self.logger.warning(f"No detector setup for timeframe {timeframe}")
            return self._get_default_regime_data()
        
        # Convert pandas Series to BarData
        bar = self._series_to_bar(bar_data, self.bar_counters[timeframe])
        self.bar_counters[timeframe] += 1
        
        # Process bar through detector
        regime_snapshot = self.detectors[timeframe].process_bar(bar)
        
        # Update latest regime
        self.latest_regimes[timeframe] = regime_snapshot
        
        self.logger.debug(
            f"Regime updated for {timeframe}: {regime_snapshot.regime} "
            f"(confidence: {regime_snapshot.confidence:.2%}, transition: {regime_snapshot.is_transition})"
        )
        
        # Return enrichment data
        return self._create_enrichment_data(regime_snapshot)
    
    def get_regime_data(self, timeframe: Optional[str] = None) -> Dict[str, Any]:
        """
        Get current regime data for enrichment.
        
        Args:
            timeframe: Specific timeframe or None for most recent across all timeframes
            
        Returns:
            Dictionary with regime, regime_confidence, and is_transition
        """
        if timeframe and timeframe in self.latest_regimes:
            return self._create_enrichment_data(self.latest_regimes[timeframe])
        elif not timeframe and self.latest_regimes:
            # Return the most recently updated regime
            latest = max(self.latest_regimes.values(), key=lambda r: r.timestamp)
            return self._create_enrichment_data(latest)
        else:
            return self._get_default_regime_data()
    
    def get_all_regimes(self) -> Dict[str, Dict[str, Any]]:
        """
        Get regime data for all timeframes.
        
        Returns:
            Dictionary mapping timeframe to regime data
        """
        result = {}
        for tf, regime in self.latest_regimes.items():
            result[tf] = self._create_enrichment_data(regime)
        return result
    
    def _warmup_detector(self, detector: RegimeDetector, df: pd.DataFrame, timeframe: str) -> None:
        """
        Warmup a detector with historical data.
        
        Args:
            detector: RegimeDetector instance
            df: Historical DataFrame
            timeframe: Timeframe identifier
        """
        self.logger.info(f"Warming up detector for {timeframe} with {len(df)} bars")
        
        for i, (idx, row) in enumerate(df.iterrows()):
            bar = self._series_to_bar(row, i, idx)
            detector.process_bar(bar)
            
            if (i + 1) % 100 == 0:
                self.logger.debug(f"Warmup progress for {timeframe}: {i+1}/{len(df)}")
    
    def _series_to_bar(self, data: pd.Series, bar_index: int, timestamp_override=None) -> BarData:
        """
        Convert pandas Series to BarData object.
        
        Args:
            data: Pandas Series with OHLC data
            bar_index: Bar index number
            timestamp_override: Optional timestamp to use instead of Series index
            
        Returns:
            BarData object
        """
        # Determine timestamp
        if timestamp_override is not None:
            timestamp = timestamp_override
        elif isinstance(data.name, pd.Timestamp):
            timestamp = data.name
        elif 'time' in data:
            timestamp = pd.to_datetime(data['time'])
        elif 'timestamp' in data:
            timestamp = pd.to_datetime(data['timestamp'])
        else:
            timestamp = pd.Timestamp.now()
        
        return BarData(
            timestamp=timestamp,
            open=float(data['open']),
            high=float(data['high']),
            low=float(data['low']),
            close=float(data['close']),
            bar_index=bar_index
        )
    
    def _create_enrichment_data(self, regime: RegimeSnapshot) -> Dict[str, Any]:
        """
        Create dictionary with regime data for enrichment.
        
        Args:
            regime: RegimeSnapshot object
            
        Returns:
            Dictionary with regime enrichment columns
        """
        return {
            'regime': regime.regime,
            'regime_confidence': float(regime.confidence),
            'is_transition': bool(regime.is_transition),
            'regime_timestamp': regime.timestamp
        }
    
    def _get_default_regime_data(self) -> Dict[str, Any]:
        """
        Get default regime data when no regime is available.
        
        Returns:
            Dictionary with default values
        """
        return {
            'regime': 'unknown',
            'regime_confidence': 0.0,
            'is_transition': False,
            'regime_timestamp': pd.Timestamp.now()
        }
    
    def get_stats(self) -> Dict[str, Dict]:
        """
        Get statistics for all timeframe detectors.
        
        Returns:
            Dictionary mapping timeframe to stats
        """
        stats = {}
        for tf, detector in self.detectors.items():
            stats[tf] = detector.stats()
        return stats
    
    def __repr__(self) -> str:
        """String representation."""
        return (
            f"RegimeManager(timeframes={list(self.detectors.keys())}, "
            f"warmup={self.warmup_bars}, persist_n={self.persist_n})"
        )