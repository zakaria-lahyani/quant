# Regime Package

## Overview

The **regime** package is an advanced market regime detection system that classifies market conditions into distinct regimes (e.g., bull_high, bear_low, neutral_medium) using multiple technical indicators, state persistence, and higher timeframe bias. It enables adaptive trading strategies that adjust behavior based on current market conditions.

## Main Features

- **Multi-Dimensional Classification**: Direction (bull/bear/neutral) + Volatility (high/medium/low) = 9 regime states
- **Multi-Timeframe Detection**: Independent regime detection across multiple timeframes
- **State Persistence**: Prevents rapid regime switching with configurable persistence
- **Higher Timeframe Bias**: Filter trades based on higher timeframe trends
- **Transition Detection**: Identifies regime changes for strategy adaptation
- **Regime-Specific Indicators**: EMA, RSI, ATR, Bollinger Bands, MACD
- **Confidence Scoring**: Quantifies regime classification certainty
- **Historical Regime Tracking**: Maintains regime history for analysis

## Package Structure

```
regime/
├── regime_manager.py           # Multi-timeframe orchestrator
├── regime_detector.py          # Core detection engine
├── regime_classifier.py        # Classification logic
├── regime_state_machine.py     # State persistence logic
├── indicator_calculator.py     # Regime-specific indicators
├── htf_regime_bias.py         # Higher timeframe bias
└── data_structure.py          # Data classes (BarData, RegimeSnapshot, etc.)
```

## Regime Classification

### Regime States

The system classifies markets into 9 states based on two dimensions:

**Direction**:
- **bull**: Uptrend (EMA rising, positive MACD, RSI > 50)
- **bear**: Downtrend (EMA falling, negative MACD, RSI < 50)
- **neutral**: Sideways (mixed signals, choppy price action)

**Volatility**:
- **high**: High volatility (ATR > threshold, wide Bollinger Bands)
- **medium**: Moderate volatility (ATR near average)
- **low**: Low volatility (ATR < threshold, tight Bollinger Bands)

**Combined Regimes**:
```
bull_high    bull_medium    bull_low
bear_high    bear_medium    bear_low
neutral_high neutral_medium neutral_low
```

### Classification Logic

```python
# Simplified classification logic
def classify_regime(indicators):
    # Direction classification
    if ema_rising and macd > 0 and rsi > 50:
        direction = "bull"
    elif ema_falling and macd < 0 and rsi < 50:
        direction = "bear"
    else:
        direction = "neutral"

    # Volatility classification
    if atr > high_threshold and bb_width > wide_threshold:
        volatility = "high"
    elif atr < low_threshold and bb_width < narrow_threshold:
        volatility = "low"
    else:
        volatility = "medium"

    return f"{direction}_{volatility}"
```

## Key Components

### RegimeManager

Multi-timeframe orchestrator managing regime detection across multiple timeframes:

```python
from app.regime.regime_manager import RegimeManager

# Initialize regime manager
regime_manager = RegimeManager(
    warmup_bars=500,           # Bars needed for warmup
    persist_n=2,              # Bars to persist regime state
    transition_bars=3,        # Transition detection window
    bb_threshold_len=200,     # Bollinger Bands threshold period
    htf_rule="ema_cross"      # Optional HTF bias rule
)

# Setup with historical data
regime_manager.setup(
    timeframes=['1', '5', '15'],
    historicals={
        '1': df_1m,
        '5': df_5m,
        '15': df_15m
    }
)

# Update with new bar
regime_data = regime_manager.update('1', new_bar_series)
# Returns: {'regime': 'bull_high', 'regime_confidence': 0.85, 'is_transition': False}
```

### RegimeDetector

Core detection engine processing bars and maintaining regime history:

```python
from app.regime.regime_detector import RegimeDetector

detector = RegimeDetector(
    warmup_bars=500,
    persist_n=2,
    transition_bars=3,
    bb_threshold_len=200
)

# Warmup with historical data
detector.warmup(historical_df)

# Process new bar
regime_snapshot = detector.process_bar(new_bar_series)

# Access regime information
print(regime_snapshot.regime)           # 'bull_high'
print(regime_snapshot.confidence)       # 0.85
print(regime_snapshot.is_transition)    # False
```

### RegimeClassifier

Pure function mapping indicator values to regime classification:

```python
from app.regime.regime_classifier import RegimeClassifier

classifier = RegimeClassifier()

# Classify based on indicators
regime = classifier.classify(
    ema_short=1.0900,
    ema_long=1.0850,
    rsi=65.0,
    atr=0.0015,
    bb_width=0.0030,
    macd=0.0005,
    high_bb_threshold=0.0025,
    low_bb_threshold=0.0010
)

print(regime)  # 'bull_high'
```

### RegimeStateMachine

State persistence preventing rapid regime switching:

```python
from app.regime.regime_state_machine import RegimeStateMachine

state_machine = RegimeStateMachine(persist_n=2)

# Update with new regime
persisted_regime = state_machine.update(
    current_regime='bull_high',
    new_regime='bull_medium'
)

# Regime only changes after persist_n consecutive changes
print(persisted_regime)  # Still 'bull_high' (first change)

# After 2nd consecutive change
persisted_regime = state_machine.update(
    current_regime='bull_high',
    new_regime='bull_medium'
)
print(persisted_regime)  # Now 'bull_medium' (persisted)
```

## Usage Examples

### Basic Setup: Single Timeframe

```python
from app.regime.regime_manager import RegimeManager

# Create regime manager
regime_manager = RegimeManager(
    warmup_bars=500,
    persist_n=2,
    transition_bars=3,
    bb_threshold_len=200
)

# Fetch historical data for warmup
historical_1m = data_manager.get_historical_data("EURUSD", "1", nbr_bars=500)

# Setup regime manager
regime_manager.setup(
    timeframes=['1'],
    historicals={'1': historical_1m}
)

# System is now ready for streaming updates
```

### Multi-Timeframe Setup

```python
# Setup for multiple timeframes
historicals = {
    '1': data_manager.get_historical_data("EURUSD", "1", nbr_bars=500),
    '5': data_manager.get_historical_data("EURUSD", "5", nbr_bars=500),
    '15': data_manager.get_historical_data("EURUSD", "15", nbr_bars=500),
}

regime_manager.setup(
    timeframes=['1', '5', '15'],
    historicals=historicals
)

# Get regime for all timeframes
regimes = regime_manager.get_all_regimes()
print(regimes)
# Output: {'1': 'bull_high', '5': 'bull_medium', '15': 'bull_low'}
```

### Processing New Bars (Live Trading)

```python
# Trading loop
while True:
    # Fetch new bar
    new_bar = data_manager.get_stream_data("EURUSD", "1", nbr_bars=1).iloc[-1]

    # Update regime
    regime_data = regime_manager.update('1', new_bar)

    # Access regime information
    print(f"Regime: {regime_data['regime']}")
    print(f"Confidence: {regime_data['regime_confidence']}")
    print(f"Transition: {regime_data['is_transition']}")

    # Use regime for trading decisions
    if regime_data['regime'] in ['bull_high', 'bull_medium']:
        # Bullish strategy
        pass
    elif regime_data['regime'] in ['bear_high', 'bear_medium']:
        # Bearish strategy
        pass
    else:
        # Neutral/low volatility strategy
        pass
```

### Integration with Indicators

```python
# Regime manager enriches indicator rows
from app.indicators.indicator_processor import IndicatorProcessor

# Process new bar with indicators
enriched_row = indicator_processor.process_new_row(
    timeframe='1',
    row=new_bar,
    regime_data=regime_data  # Adds regime fields
)

# Enriched row now contains:
print(enriched_row['regime'])            # 'bull_high'
print(enriched_row['regime_confidence']) # 0.85
print(enriched_row['is_transition'])     # False
print(enriched_row['ema_20'])           # 1.0900 (indicator)
print(enriched_row['rsi'])              # 65.0 (indicator)
```

### Higher Timeframe Bias

```python
# Initialize with HTF bias rule
regime_manager = RegimeManager(
    warmup_bars=500,
    persist_n=2,
    transition_bars=3,
    bb_threshold_len=200,
    htf_rule="ema_cross"  # Filter based on EMA crossover
)

# Setup with HTF data
regime_manager.setup(
    timeframes=['1', '5', '15'],
    historicals=historicals
)

# HTF bias filters lower timeframe signals
# Example: Only take long trades if H1 is also bullish
```

## Regime-Specific Indicators

The package calculates regime-specific technical indicators:

### EMA (Exponential Moving Average)
- **Purpose**: Trend direction
- **Calculation**: Weighted average with more emphasis on recent prices
- **Usage**: EMA crossovers indicate trend changes

### RSI (Relative Strength Index)
- **Purpose**: Momentum and overbought/oversold conditions
- **Range**: 0-100
- **Thresholds**: RSI > 70 (overbought), RSI < 30 (oversold)

### ATR (Average True Range)
- **Purpose**: Volatility measurement
- **Usage**: High ATR = high volatility, Low ATR = low volatility
- **Normalization**: ATR / price for cross-asset comparison

### Bollinger Bands Width
- **Purpose**: Volatility measurement
- **Calculation**: (Upper Band - Lower Band) / Middle Band
- **Usage**: Wide bands = high volatility, Narrow bands = low volatility

### MACD (Moving Average Convergence Divergence)
- **Purpose**: Trend direction and momentum
- **Components**: MACD line, Signal line, Histogram
- **Usage**: MACD > 0 (bullish), MACD < 0 (bearish)

## Configuration

### RegimeManager Parameters

```python
regime_manager = RegimeManager(
    warmup_bars=500,           # Number of bars for warmup (default: 500)
    persist_n=2,               # Persistence threshold (default: 2)
    transition_bars=3,         # Transition detection window (default: 3)
    bb_threshold_len=200,      # Bollinger Bands threshold period (default: 200)
    htf_rule="ema_cross"       # HTF bias rule (optional)
)
```

**Parameters Explained**:

- **warmup_bars**: Historical data needed for indicator warmup
  - Higher values = more stable but slower initialization
  - Recommended: 500+ for reliable regime detection

- **persist_n**: Number of consecutive bars required to change regime
  - Higher values = less sensitive, fewer false regime changes
  - Lower values = more responsive but more noise
  - Recommended: 2-5

- **transition_bars**: Window for transition detection
  - Identifies when regime is changing
  - Useful for reducing trades during uncertain periods
  - Recommended: 3-5

- **bb_threshold_len**: Period for Bollinger Bands threshold calculation
  - Determines high/low volatility thresholds
  - Longer periods = more stable thresholds
  - Recommended: 200+

- **htf_rule**: Higher timeframe bias rule
  - Options: "ema_cross", "trend_alignment", None
  - Filters lower TF signals based on higher TF regime
  - Optional but recommended for trend-following

## Regime-Based Strategy Examples

### Trend-Following Strategy

```python
# Only trade in the direction of the regime
if regime == 'bull_high' or regime == 'bull_medium':
    # Look for long entries
    if entry_signal:
        take_long_trade()

elif regime == 'bear_high' or regime == 'bear_medium':
    # Look for short entries
    if entry_signal:
        take_short_trade()

else:
    # Neutral or low volatility - avoid trading
    pass
```

### Mean-Reversion Strategy

```python
# Trade reversals in neutral/ranging markets
if regime == 'neutral_medium' or regime == 'neutral_low':
    if rsi > 70:
        # Overbought - look for shorts
        take_short_trade()
    elif rsi < 30:
        # Oversold - look for longs
        take_long_trade()
```

### Volatility-Adaptive Strategy

```python
# Adjust position sizing based on volatility
if 'high' in regime:
    # High volatility - reduce position size
    position_size *= 0.5
elif 'low' in regime:
    # Low volatility - increase position size
    position_size *= 1.5
```

### Transition-Aware Strategy

```python
# Avoid trading during regime transitions
if regime_data['is_transition']:
    # Market is transitioning - wait for stability
    print("Regime transition detected, avoiding trades")
else:
    # Stable regime - trade normally
    evaluate_entry_signals()
```

## Integration Points

### With Indicators Package

```python
# Indicators package enriches rows with regime data
from app.indicators.indicator_processor import IndicatorProcessor

indicator_processor = IndicatorProcessor(
    configs=configs,
    historicals=historicals,
    is_bulk=False
)

# Process new row with regime enrichment
enriched_row = indicator_processor.process_new_row(
    timeframe='1',
    row=new_bar,
    regime_data=regime_manager.update('1', new_bar)  # Regime data added
)
```

### With Strategy Builder

```python
# Strategies can use regime in conditions
# YAML configuration
entry:
  long:
    conditions:
      - signal: regime
        operator: IN
        value: ["bull_high", "bull_medium"]
        timeframe: "1"
      - signal: ema_20
        operator: GREATER_THAN
        value: ema_50
        timeframe: "1"
```

### With Entry Manager

```python
# Different risk parameters per regime
if regime in ['bull_high', 'bear_high']:
    # High volatility - wider stops
    stop_loss_pips = 100
elif regime in ['bull_low', 'bear_low']:
    # Low volatility - tighter stops
    stop_loss_pips = 30
else:
    # Medium volatility - standard stops
    stop_loss_pips = 50
```

## Advanced Features

### Regime Confidence Scoring

```python
# Confidence indicates regime strength
regime_data = regime_manager.update('1', new_bar)

confidence = regime_data['regime_confidence']

if confidence > 0.8:
    # High confidence - strong regime
    print("Strong regime signal")
elif confidence < 0.5:
    # Low confidence - weak/uncertain regime
    print("Weak regime signal, be cautious")
```

### Transition Detection

```python
# Detect regime changes
if regime_data['is_transition']:
    # Regime is changing
    # - Close existing positions
    # - Avoid new entries
    # - Tighten stops
    print("Regime transition detected")
```

### Historical Regime Access

```python
# Access regime history
detector = regime_manager.detectors['1']
regime_history = detector.regime_history

# Analyze recent regimes
recent_regimes = regime_history[-10:]  # Last 10 regimes
print(f"Recent regime changes: {recent_regimes}")
```

### Multi-Timeframe Alignment

```python
# Check if all timeframes agree
regimes = regime_manager.get_all_regimes()

if all('bull' in regime for regime in regimes.values()):
    # All timeframes bullish - strong signal
    print("All timeframes bullish")
elif all('bear' in regime for regime in regimes.values()):
    # All timeframes bearish - strong signal
    print("All timeframes bearish")
else:
    # Mixed signals - be cautious
    print("Mixed regime signals across timeframes")
```

## Best Practices

### 1. Sufficient Warmup

```python
# Ensure enough data for reliable regime detection
warmup_bars = max(500, longest_indicator_period * 3)

regime_manager = RegimeManager(warmup_bars=warmup_bars)
```

### 2. Appropriate Persistence

```python
# Lower timeframes need more persistence
if timeframe == '1':
    persist_n = 5  # More persistent for 1-minute
elif timeframe == '60':
    persist_n = 2  # Less persistent for 1-hour
```

### 3. Regime-Specific Parameters

```python
# Adjust strategy based on regime
if regime == 'bull_high':
    # Aggressive long strategy
    profit_target = 100
    stop_loss = 50
elif regime == 'neutral_low':
    # Conservative range-bound strategy
    profit_target = 30
    stop_loss = 20
```

### 4. Validate Regime Changes

```python
# Log regime changes for analysis
previous_regime = None
current_regime = regime_data['regime']

if previous_regime and previous_regime != current_regime:
    logger.info(
        f"Regime changed: {previous_regime} → {current_regime} | "
        f"Confidence: {regime_data['regime_confidence']:.2f}"
    )

previous_regime = current_regime
```

## Troubleshooting

### Frequent Regime Changes

**Issue**: Regime switching too frequently
**Solutions**:
1. Increase `persist_n` parameter
2. Increase `transition_bars` window
3. Use higher timeframe regimes
4. Filter on regime confidence

### Delayed Regime Detection

**Issue**: Regime changes detected too late
**Solutions**:
1. Decrease `persist_n` parameter
2. Use more responsive indicators
3. Lower `warmup_bars` if appropriate
4. Check indicator calculation correctness

### Inaccurate Classification

**Issue**: Regime doesn't match market conditions
**Solutions**:
1. Increase `warmup_bars` for better indicator accuracy
2. Adjust Bollinger Bands threshold period
3. Validate historical data quality
4. Review classification thresholds

## Performance Considerations

1. **Warmup Time**: Initial warmup processes full historical dataset
2. **Update Speed**: Incremental updates ~5-10ms per timeframe
3. **Memory Usage**: ~10 MB per timeframe with 500-bar history
4. **CPU Usage**: Minimal after warmup (incremental calculations)

## Testing

```python
import pytest
from app.regime.regime_manager import RegimeManager

def test_regime_manager():
    """Test regime manager initialization and updates."""

    # Mock data
    mock_df = create_mock_ohlc_data(bars=500)

    # Create regime manager
    regime_manager = RegimeManager(
        warmup_bars=500,
        persist_n=2,
        transition_bars=3
    )

    # Setup
    regime_manager.setup(
        timeframes=['1'],
        historicals={'1': mock_df}
    )

    # Update with new bar
    new_bar = mock_df.iloc[-1]
    regime_data = regime_manager.update('1', new_bar)

    # Assertions
    assert 'regime' in regime_data
    assert 'regime_confidence' in regime_data
    assert 'is_transition' in regime_data
    assert regime_data['regime'] in [
        'bull_high', 'bull_medium', 'bull_low',
        'bear_high', 'bear_medium', 'bear_low',
        'neutral_high', 'neutral_medium', 'neutral_low'
    ]
    assert 0 <= regime_data['regime_confidence'] <= 1
```

## Conclusion

The regime package provides sophisticated market classification enabling adaptive trading strategies. Its multi-timeframe support, state persistence, and comprehensive indicator calculations make it an essential tool for regime-based algorithmic trading systems.
