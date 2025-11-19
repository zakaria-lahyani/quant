# Strategy Builder Package

## Overview

The **strategy_builder** package is a flexible, configuration-driven strategy evaluation engine supporting complex condition trees, multiple timeframes, and directional (long/short) entry/exit rules. It uses Pydantic models for validation and dependency injection for testability, enabling traders to define sophisticated trading strategies entirely through YAML configuration files.

## Main Features

- **Configuration-Driven**: Define complete strategies in YAML without coding
- **Complex Condition Trees**: Support for AND/OR/NOT logic with nested conditions
- **Multi-Timeframe Analysis**: Evaluate conditions across multiple timeframes
- **Rich Operators**: 15+ comparison operators (GREATER_THAN, CROSSES_ABOVE, IN, BETWEEN, etc.)
- **Directional Signals**: Separate long/short entry and exit logic
- **Schedule-Based Activation**: Time-based strategy activation with day/hour filtering
- **Pydantic Validation**: Type safety and automatic validation
- **Dependency Injection**: Testable and maintainable architecture

## Package Structure

```
strategy_builder/
├── core/
│   ├── services/
│   │   ├── engine.py          # Main strategy evaluation engine
│   │   ├── executor.py        # Single strategy executor
│   │   └── loader.py          # Strategy loader from YAML
│   ├── evaluators/
│   │   ├── condition.py       # Individual condition evaluation
│   │   └── logic.py           # Logical tree evaluation
│   ├── domain/
│   │   ├── models.py          # Pydantic models (TradingStrategy, etc.)
│   │   └── enums.py           # Enums (TimeFrameEnum, OperatorEnum, etc.)
│   └── factories/
│       └── evaluator.py       # Evaluator factory with DI
├── config/
│   └── ...                    # Configuration utilities
├── data/
│   └── ...                    # Data handling
└── infrastructure/
    └── ...                    # External integrations
```

## Key Components

### StrategyEngine

Main facade for strategy evaluation managing multiple strategies:

```python
from app.strategy_builder.core.services.engine import StrategyEngine
from app.strategy_builder.core.services.loader import YAMLStrategyLoader

# Load strategies from YAML files
loader = YAMLStrategyLoader(folder_path="config/strategies/eurusd")

# Create engine
engine = StrategyEngine(
    strategy_loader=loader,
    evaluator_factory=evaluator_factory,
    logger=logger
)

# Evaluate all strategies
result = engine.evaluate(recent_rows)

# Access signals
for name, eval_result in result.strategies.items():
    if eval_result.entry.long:
        print(f"{name}: LONG signal")
    if eval_result.exit.short:
        print(f"{name}: EXIT SHORT signal")
```

### StrategyExecutor

Evaluates a single strategy's entry/exit conditions:

```python
from app.strategy_builder.core.services.executor import StrategyExecutor

executor = StrategyExecutor(
    strategy=strategy,
    evaluator_factory=evaluator_factory,
    logger=logger
)

# Evaluate strategy
result = executor.evaluate(recent_rows)

print(f"Long entry: {result.entry.long}")
print(f"Short entry: {result.entry.short}")
print(f"Long exit: {result.exit.long}")
print(f"Short exit: {result.exit.short}")
```

### TradingStrategy (Pydantic Model)

Complete strategy definition with validation:

```python
from app.strategy_builder.core.domain.models import TradingStrategy

strategy = TradingStrategy(
    name="my_strategy",
    timeframes=["1", "5"],
    activation=ActivationRules(...),
    entry=EntryRules(...),
    exit=ExitRules(...),
    risk=RiskManagement(...)
)

# Automatic validation ensures:
# - All required fields present
# - Type correctness
# - Logical consistency
```

## Strategy Configuration

### Complete Strategy YAML Example

```yaml
# config/strategies/eurusd/trend_follower.yaml
name: trend_follower
timeframes: ["1", "5"]

# Activation rules
activation:
  enabled: true
  schedule:
    days: [MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY]
    hours: "00:00-23:59"

# Entry rules
entry:
  # Long entry conditions
  long:
    mode: all  # or: any, tree
    conditions:
      - signal: ema_20
        operator: GREATER_THAN
        value: ema_50
        timeframe: "1"
        lookback: 1  # Current bar
      - signal: rsi
        operator: BETWEEN
        value: [40, 70]
        timeframe: "1"
        lookback: 1
      - signal: regime
        operator: IN
        value: ["bull_high", "bull_medium"]
        timeframe: "5"
        lookback: 1

  # Short entry conditions
  short:
    mode: all
    conditions:
      - signal: ema_20
        operator: LESS_THAN
        value: ema_50
        timeframe: "1"
        lookback: 1
      - signal: rsi
        operator: BETWEEN
        value: [30, 60]
        timeframe: "1"
        lookback: 1
      - signal: regime
        operator: IN
        value: ["bear_high", "bear_medium"]
        timeframe: "5"
        lookback: 1

# Exit rules
exit:
  # Long exit conditions
  long:
    mode: any  # Exit if ANY condition is true
    conditions:
      - signal: ema_20
        operator: CROSSES_BELOW
        value: ema_50
        timeframe: "1"
        lookback: 2  # Check last 2 bars for cross
      - signal: rsi
        operator: GREATER_THAN
        value: 80
        timeframe: "1"
        lookback: 1
    time_based:
      max_duration: "24h"  # Exit after 24 hours

  # Short exit conditions
  short:
    mode: any
    conditions:
      - signal: ema_20
        operator: CROSSES_ABOVE
        value: ema_50
        timeframe: "1"
        lookback: 2
      - signal: rsi
        operator: LESS_THAN
        value: 20
        timeframe: "1"
        lookback: 1
    time_based:
      max_duration: "24h"

# Risk management
risk:
  position_sizing:
    type: percentage
    value: 1.0  # 1% of account
  sl:
    type: monetary
    value: 500.0  # $500 max loss
  tp:
    type: multi_target
    targets:
      - value: 1.0   # 1% profit
        percent: 60  # Close 60%
        move_stop: true
      - value: 2.0   # 2% profit
        percent: 40  # Close 40%
        move_stop: false
```

## Condition Modes

Strategies support three modes for combining conditions:

### 1. ALL Mode (AND Logic)

All conditions must be true:

```yaml
entry:
  long:
    mode: all
    conditions:
      - signal: ema_20
        operator: GREATER_THAN
        value: ema_50
      - signal: rsi
        operator: LESS_THAN
        value: 70
      - signal: regime
        operator: EQUALS
        value: "bull_high"

# Result: Enter long ONLY if ALL three conditions are true
```

### 2. ANY Mode (OR Logic)

At least one condition must be true:

```yaml
exit:
  long:
    mode: any
    conditions:
      - signal: rsi
        operator: GREATER_THAN
        value: 80  # Overbought
      - signal: ema_20
        operator: CROSSES_BELOW
        value: ema_50  # Trend reversal
      - signal: atr
        operator: GREATER_THAN
        value: 0.0050  # High volatility

# Result: Exit if ANY condition is true
```

### 3. TREE Mode (Complex Logic)

Custom logical trees with AND/OR/NOT:

```yaml
entry:
  long:
    mode: tree
    tree:
      operator: AND
      children:
        - operator: OR
          children:
            - condition:
                signal: ema_20
                operator: GREATER_THAN
                value: ema_50
            - condition:
                signal: macd
                operator: GREATER_THAN
                value: 0
        - operator: AND
          children:
            - condition:
                signal: rsi
                operator: BETWEEN
                value: [40, 70]
            - operator: NOT
              child:
                condition:
                  signal: regime
                  operator: EQUALS
                  value: "neutral_low"

# Result: Complex nested logic
# (EMA crossover OR MACD positive) AND (RSI in range AND NOT neutral_low regime)
```

## Available Operators

### Comparison Operators

```yaml
# Numeric comparisons
EQUALS: "=="
NOT_EQUALS: "!="
GREATER_THAN: ">"
LESS_THAN: "<"
GREATER_THAN_OR_EQUAL: ">="
LESS_THAN_OR_EQUAL: "<="

# Examples
- signal: rsi
  operator: GREATER_THAN
  value: 70

- signal: close
  operator: LESS_THAN_OR_EQUAL
  value: ema_20
```

### Range Operators

```yaml
# Check if value is within range
BETWEEN:
  - signal: rsi
    operator: BETWEEN
    value: [30, 70]  # 30 <= RSI <= 70

# Check if value is outside range
NOT_BETWEEN:
  - signal: atr
    operator: NOT_BETWEEN
    value: [0.0010, 0.0020]
```

### Set Operators

```yaml
# Check if value is in set
IN:
  - signal: regime
    operator: IN
    value: ["bull_high", "bull_medium", "neutral_high"]

# Check if value is not in set
NOT_IN:
  - signal: regime
    operator: NOT_IN
    value: ["neutral_low", "bear_low"]
```

### Crossover Operators

```yaml
# Signal crosses above reference
CROSSES_ABOVE:
  - signal: ema_20
    operator: CROSSES_ABOVE
    value: ema_50
    lookback: 2  # Check last 2 bars

# Signal crosses below reference
CROSSES_BELOW:
  - signal: ema_20
    operator: CROSSES_BELOW
    value: ema_50
    lookback: 2
```

### Boolean Operators

```yaml
# Direct boolean check
IS_TRUE:
  - signal: is_bullish_candle
    operator: IS_TRUE

IS_FALSE:
  - signal: is_bearish_candle
    operator: IS_FALSE
```

## Lookback and Timeframe Features

### Lookback

Access historical values for pattern detection:

```yaml
conditions:
  # Current bar (default)
  - signal: rsi
    operator: GREATER_THAN
    value: 70
    lookback: 1  # Current bar

  # Previous bar
  - signal: rsi
    operator: LESS_THAN
    value: 30
    lookback: 2  # Previous bar

  # 3 bars ago
  - signal: close
    operator: GREATER_THAN
    value: ema_20
    lookback: 3  # 3 bars ago
```

### Multi-Timeframe Conditions

Combine signals from different timeframes:

```yaml
entry:
  long:
    mode: all
    conditions:
      # 1-minute timeframe
      - signal: ema_20
        operator: GREATER_THAN
        value: ema_50
        timeframe: "1"

      # 5-minute timeframe (higher TF confirmation)
      - signal: regime
        operator: IN
        value: ["bull_high", "bull_medium"]
        timeframe: "5"

      # 15-minute timeframe (overall trend)
      - signal: ema_20
        operator: GREATER_THAN
        value: ema_50
        timeframe: "15"

# Result: Only enter if aligned across all timeframes
```

## Activation Rules

Control when strategies are active:

### Schedule-Based Activation

```yaml
activation:
  enabled: true
  schedule:
    # Trading days
    days: [MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY]

    # Trading hours (24-hour format)
    hours: "08:00-16:00"  # Trade only during market hours
```

### Advanced Scheduling

```yaml
# Multiple time windows
activation:
  enabled: true
  schedule:
    days: [MONDAY, TUESDAY, WEDNESDAY, THURSDAY]
    hours: "08:00-12:00,13:00-17:00"  # Avoid lunch hour

# Weekend trading
activation:
  enabled: true
  schedule:
    days: [SATURDAY, SUNDAY]
    hours: "00:00-23:59"

# Disable strategy
activation:
  enabled: false  # Strategy won't run
```

## Usage Examples

### Basic Trend-Following Strategy

```python
from app.strategy_builder.core.services.engine import StrategyEngine

# Load strategies
engine = StrategyEngine(...)

# Get recent indicator data
recent_rows = indicator_processor.get_recent_rows()

# Evaluate strategies
result = engine.evaluate(recent_rows)

# Check for signals
for strategy_name, eval_result in result.strategies.items():
    if eval_result.entry.long:
        logger.info(f"{strategy_name}: Long entry signal")

        # Calculate risk parameters
        entry_decision = entry_manager.calculate_entry_decision(
            strategy_name=strategy_name,
            symbol="EURUSD",
            direction="long",
            entry_price=current_price,
            decision_time=datetime.now(),
            market_data=recent_rows,
            account_balance=account_balance
        )

        # Execute trade
        trader.execute_entry(entry_decision)
```

### Mean-Reversion Strategy

```yaml
# config/strategies/eurusd/mean_reversion.yaml
name: mean_reversion
timeframes: ["1"]

entry:
  long:
    mode: all
    conditions:
      # Regime: neutral or ranging
      - signal: regime
        operator: IN
        value: ["neutral_medium", "neutral_low"]
        timeframe: "1"

      # Price: oversold
      - signal: rsi
        operator: LESS_THAN
        value: 30
        timeframe: "1"

      # Price: near lower Bollinger Band
      - signal: close
        operator: LESS_THAN
        value: bb_lower
        timeframe: "1"

  short:
    mode: all
    conditions:
      - signal: regime
        operator: IN
        value: ["neutral_medium", "neutral_low"]
        timeframe: "1"

      - signal: rsi
        operator: GREATER_THAN
        value: 70
        timeframe: "1"

      - signal: close
        operator: GREATER_THAN
        value: bb_upper
        timeframe: "1"

exit:
  long:
    mode: any
    conditions:
      # Exit at middle band
      - signal: close
        operator: GREATER_THAN
        value: bb_middle
        timeframe: "1"

      # Exit if overbought
      - signal: rsi
        operator: GREATER_THAN
        value: 70
        timeframe: "1"
```

### Breakout Strategy

```yaml
# config/strategies/eurusd/breakout.yaml
name: breakout
timeframes: ["5", "15"]

entry:
  long:
    mode: all
    conditions:
      # Volatility increasing
      - signal: atr
        operator: GREATER_THAN
        value: 0.0020
        timeframe: "5"

      # Price breaks above resistance
      - signal: high
        operator: CROSSES_ABOVE
        value: bb_upper
        timeframe: "5"
        lookback: 2

      # Higher timeframe confirmation
      - signal: regime
        operator: IN
        value: ["bull_high", "neutral_high"]
        timeframe: "15"

      # Volume confirmation
      - signal: volume
        operator: GREATER_THAN
        value: 1500
        timeframe: "5"

exit:
  long:
    mode: any
    conditions:
      # Price returns inside bands
      - signal: close
        operator: LESS_THAN
        value: bb_upper
        timeframe: "5"

      # Volatility decreases
      - signal: atr
        operator: LESS_THAN
        value: 0.0015
        timeframe: "5"

    time_based:
      max_duration: "4h"  # Exit after 4 hours
```

## Integration Points

### With Indicators Package

```python
# Strategies use indicator values from recent rows
from app.indicators.indicator_processor import IndicatorProcessor

indicator_processor = IndicatorProcessor(...)
recent_rows = indicator_processor.get_recent_rows()

# Recent rows contain all indicator values
# Example: recent_rows['1'][-1]['ema_20'], recent_rows['1'][-1]['rsi']

# Strategy evaluates conditions against these values
result = strategy_engine.evaluate(recent_rows)
```

### With Entry Manager

```python
# Entry manager uses strategy evaluation results
from app.entry_manager.manager import EntryManager

# Evaluate strategies
strategy_results = strategy_engine.evaluate(recent_rows)

# Generate entry decisions for signals
for strategy_name, eval_result in strategy_results.strategies.items():
    if eval_result.entry.long:
        entry_decision = entry_manager.calculate_entry_decision(
            strategy_name=strategy_name,
            symbol="EURUSD",
            direction="long",
            entry_price=current_price,
            decision_time=datetime.now(),
            market_data=recent_rows,
            account_balance=account_balance
        )
```

### With Trader Package

```python
# Complete flow: Strategy → Entry Manager → Trader
from app.trader.trade_executor import TradeExecutor

# 1. Evaluate strategies
signals = strategy_engine.evaluate(recent_rows)

# 2. Calculate risk parameters
trades = []
for name, result in signals.strategies.items():
    if result.entry.long:
        entry = entry_manager.calculate_entry_decision(...)
        trades.append(entry)

# 3. Execute trades
context = trade_executor.execute_trading_cycle(trades, date_helper)
```

## Best Practices

### 1. Use Descriptive Strategy Names

```yaml
# Good
name: eurusd_trend_following_ema_cross

# Avoid
name: strategy1
```

### 2. Validate Strategies at Startup

```python
# Load and validate all strategies at initialization
try:
    engine = StrategyEngine(strategy_loader=loader, ...)
    logger.info("All strategies loaded and validated")
except ValidationError as e:
    logger.error(f"Strategy validation failed: {e}")
    raise
```

### 3. Use Appropriate Condition Modes

```yaml
# Entry: Use 'all' for strict filtering
entry:
  long:
    mode: all  # All conditions must be true

# Exit: Use 'any' for quick exits
exit:
  long:
    mode: any  # Exit if any condition triggers
```

### 4. Multi-Timeframe Confirmation

```yaml
# Confirm on lower TF, validate on higher TF
entry:
  long:
    mode: all
    conditions:
      # Entry signal on 1-minute
      - signal: ema_20
        operator: CROSSES_ABOVE
        value: ema_50
        timeframe: "1"

      # Trend confirmation on 5-minute
      - signal: regime
        operator: IN
        value: ["bull_high", "bull_medium"]
        timeframe: "5"
```

### 5. Log Strategy Evaluations

```python
# Log evaluation results for debugging
result = engine.evaluate(recent_rows)

for name, eval_result in result.strategies.items():
    logger.debug(
        f"{name} | "
        f"Long Entry: {eval_result.entry.long} | "
        f"Short Entry: {eval_result.entry.short} | "
        f"Long Exit: {eval_result.exit.long} | "
        f"Short Exit: {eval_result.exit.short}"
    )
```

## Troubleshooting

### Strategy Not Triggering

**Issue**: Strategy never generates signals
**Solutions**:
1. Check activation schedule matches current time
2. Verify required indicators exist in recent_rows
3. Validate timeframe data is available
4. Review condition logic (are they too strict?)
5. Check lookback values are within available data

### Validation Errors

**Issue**: Strategy fails to load
**Solutions**:
1. Verify YAML syntax is correct
2. Check all required fields are present
3. Ensure enum values match (e.g., TimeFrameEnum values)
4. Validate risk management configuration
5. Review Pydantic model requirements

### Missing Indicator Data

**Issue**: Condition evaluation fails due to missing indicators
**Solutions**:
1. Ensure indicators are enabled in indicator config
2. Verify indicator names match exactly (case-sensitive)
3. Check timeframe data is being processed
4. Validate recent_rows contains required timeframes

## Performance Considerations

1. **Strategy Count**: 10-20 strategies per symbol is reasonable
2. **Condition Complexity**: Complex trees may take longer to evaluate
3. **Lookback Depth**: Deeper lookbacks require more data retention
4. **Multi-Timeframe**: Each additional timeframe adds evaluation overhead

## Testing

```python
import pytest
from app.strategy_builder.core.services.engine import StrategyEngine

def test_strategy_evaluation():
    """Test strategy evaluation with mock data."""

    # Mock recent rows
    mock_recent_rows = {
        '1': [
            {'ema_20': 1.0900, 'ema_50': 1.0850, 'rsi': 65, 'regime': 'bull_high'},
            {'ema_20': 1.0905, 'ema_50': 1.0855, 'rsi': 66, 'regime': 'bull_high'},
        ]
    }

    # Create engine with mock strategies
    engine = StrategyEngine(...)

    # Evaluate
    result = engine.evaluate(mock_recent_rows)

    # Assertions
    assert 'my_strategy' in result.strategies
    assert result.strategies['my_strategy'].entry.long == True
```

## Conclusion

The strategy_builder package provides a powerful, flexible framework for defining trading strategies through configuration. Its rich operator set, multi-timeframe support, and comprehensive validation make it suitable for both simple and highly sophisticated trading systems.
