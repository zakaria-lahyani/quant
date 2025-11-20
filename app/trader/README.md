# Trader Package

## Overview

The **trader** package is the trade execution layer managing order placement, position monitoring, risk limits, duplicate filtering, and trading restrictions. It implements clean separation of concerns with specialized components, ensuring safe and reliable trade execution with comprehensive risk controls.

## Main Features

- **Complete Trade Execution**: Order placement, modification, and closure
- **Risk Management**: Daily loss limits, position limits, catastrophic loss protection
- **Duplicate Prevention**: Prevents duplicate trades based on existing positions/orders
- **Trading Restrictions**: News-based and time-based trading blocks
- **Position Splitting**: Split orders into multiple smaller orders for better fills
- **Order Scaling**: Pyramid or equal entry spacing
- **Exit Management**: Automatic position closure based on signals
- **Context Tracking**: Maintains current trading state (authorization, P&L, risk)

## Package Structure

```
trader/
├── base_trader.py                  # Abstract trader interface
├── live_trader.py                  # MT5 implementation
├── trade_executor.py               # Main orchestrator
├── trading_context.py              # State management
├── executor_builder.py             # Builder pattern for setup
├── components/
│   ├── exit_manager.py            # Exit processing
│   ├── duplicate_filter.py        # Duplicate prevention
│   ├── risk_monitor.py            # P&L and loss limits
│   └── order_executor.py          # Order placement
├── managers/
│   └── restriction_manager.py     # Trading restrictions
└── risk_manager/
    └── ...                        # Risk management components
```

## Key Components

### TradeExecutor

Main orchestrator coordinating the complete trading cycle:

```python
from app.trader.trade_executor import TradeExecutor

# Execute trading cycle
context = trade_executor.execute_trading_cycle(
    trades=entry_decisions,  # List of EntryDecision objects
    date_helper=date_helper
)

# Check results
if context.can_trade():
    print("Trading authorized")
else:
    print(f"Trading blocked: {context.block_reasons}")

print(f"Daily P&L: ${context.daily_pnl:.2f}")
print(f"Risk breached: {context.risk_breached}")
```

### LiveTrader

MT5 API wrapper for actual trading operations:

```python
from app.trader.live_trader import LiveTrader

trader = LiveTrader(client=mt5_client, logger=logger)

# Create market order
result = trader.create_market_order(
    symbol="EURUSD",
    direction="long",
    volume=0.1,
    stop_loss=1.0850,
    take_profit=1.0950,
    magic_number=123456,
    comment="Strategy ABC"
)

# Get open positions
positions = trader.get_open_positions()

# Close position
trader.close_position(ticket=12345678)
```

### TradingContext

Maintains current trading state:

```python
from app.trader.trading_context import TradingContext

context = TradingContext()

# Update state
context.update_authorization(authorized=True, reasons=[])
context.update_market_state(market_open=True)
context.update_daily_pnl(pnl=150.50)
context.update_risk_breach(breached=False)

# Check state
if context.can_trade():
    # All conditions met for trading
    execute_trades()
else:
    # Trading blocked
    logger.warning(f"Cannot trade: {context.block_reasons}")
```

## Complete Trading Cycle

The `TradeExecutor` orchestrates a complete trading cycle:

### Cycle Steps

1. **Update Context**: Refresh market state and authorization
2. **Apply Restrictions**: Check news blocks, market closing, suspensions
3. **Process Exits**: Close positions based on exit signals
4. **Check Risk**: Monitor daily P&L and loss limits
5. **Process Entries**: Execute new trades if authorized
6. **Return Context**: Provide updated trading state

### Example Flow

```python
# 1. Initialize executor
trade_executor = ExecutorBuilder.build_from_config(
    config=env_config,
    client=mt5_client,
    logger=logger
)

# 2. Generate trade decisions
entry_decisions = []  # From entry manager

for strategy_name, eval_result in strategy_results.strategies.items():
    if eval_result.entry.long:
        entry = entry_manager.calculate_entry_decision(
            strategy_name=strategy_name,
            symbol="EURUSD",
            direction="long",
            entry_price=current_price,
            decision_time=datetime.now(),
            market_data=recent_rows,
            account_balance=account_balance
        )
        entry_decisions.append(entry)

# 3. Execute trading cycle
context = trade_executor.execute_trading_cycle(
    trades=entry_decisions,
    date_helper=date_helper
)

# 4. Check results
logger.info(f"Trading authorized: {context.can_trade()}")
logger.info(f"Daily P&L: ${context.daily_pnl:.2f}")
logger.info(f"Positions: {len(context.open_positions)}")
```

## Components

### ExitManager

Processes exit signals and closes positions:

```python
from app.trader.components.exit_manager import ExitManager

exit_manager = ExitManager(trader=live_trader, logger=logger)

# Process exit signals
exit_manager.process_exits(
    exit_signals=exit_decisions,  # List of ExitDecision objects
    open_positions=current_positions
)

# Closes matching positions automatically
```

### DuplicateFilter

Prevents duplicate trades:

```python
from app.trader.components.duplicate_filter import DuplicateFilter

duplicate_filter = DuplicateFilter(logger=logger)

# Filter duplicates
filtered_entries = duplicate_filter.filter_duplicates(
    entries=entry_decisions,
    open_positions=current_positions,
    pending_orders=current_orders
)

# Returns only non-duplicate entries
```

**Duplicate Detection Logic**:
- Checks if position already exists for same strategy and direction
- Checks if pending order exists for same strategy and direction
- Filters out duplicates to prevent over-exposure

### RiskMonitor

Monitors P&L and enforces loss limits:

```python
from app.trader.components.risk_monitor import RiskMonitor

risk_monitor = RiskMonitor(
    daily_loss_limit=1000.0,  # $1000 max daily loss
    logger=logger
)

# Calculate daily P&L
daily_pnl = risk_monitor.calculate_daily_pnl(
    positions=open_positions,
    history=today_history
)

# Check if risk breached
risk_breached = risk_monitor.check_risk_breach(daily_pnl)

if risk_breached:
    logger.error(f"Daily loss limit breached: ${daily_pnl:.2f}")
    # Stop trading for the day
```

### OrderExecutor

Executes entry orders with splitting and scaling:

```python
from app.trader.components.order_executor import OrderExecutor

order_executor = OrderExecutor(
    trader=live_trader,
    position_split=3,          # Split into 3 orders
    scaling_type="equal",      # or "pyramid"
    entry_spacing=0.0005,      # 5 pips between orders
    risk_per_group=500.0,      # $500 risk per group
    logger=logger
)

# Execute entry with splitting
order_executor.execute_entry(entry_decision)

# Creates 3 orders:
# Order 1: 0.033 lots at 1.0900
# Order 2: 0.033 lots at 1.0895 (5 pips lower)
# Order 3: 0.033 lots at 1.0890 (10 pips lower)
```

### RestrictionManager

Applies trading restrictions:

```python
from app.trader.managers.restriction_manager import RestrictionManager

restriction_manager = RestrictionManager(
    config_folder_path="./config/restrictions",
    default_close_time="16:55",
    news_restriction_duration=5,      # Minutes before/after news
    market_close_restriction_duration=5,  # Minutes before market close
    logger=logger
)

# Check if trading is restricted
is_restricted, reason = restriction_manager.is_restricted(
    current_time=datetime.now(),
    symbol="EURUSD"
)

if is_restricted:
    logger.warning(f"Trading restricted: {reason}")
```

**Restriction Types**:
1. **News Events**: Block trading around high-impact news
2. **Market Closing**: Block trading near market close
3. **Manual Suspensions**: Manually configured trading blocks

## Configuration

### Environment Variables

```bash
# Risk Management
DAILY_LOSS_LIMIT=1000.0          # Maximum daily loss in USD
MAX_POSITIONS=10                  # Maximum concurrent positions

# Order Splitting
POSITION_SPLIT=3                  # Split orders into N parts
SCALING_TYPE=equal                # or "pyramid"
ENTRY_SPACING=0.0005              # Price spacing (5 pips for EURUSD)

# Risk Per Group
RISK_PER_GROUP=500.0              # Risk per order group

# Restrictions
RESTRICTION_CONF_FOLDER_PATH=./config/restrictions
DEFAULT_CLOSE_TIME=16:55
NEWS_RESTRICTION_DURATION=5       # Minutes
MARKET_CLOSE_RESTRICTION_DURATION=5  # Minutes

# Trading Mode
TRADE_MODE=live                   # or "backtest"
```

### Restriction Configuration

```yaml
# config/restrictions/eurusd.yaml
symbol: EURUSD

# News events
news_events:
  - datetime: "2024-01-15 14:30:00"
    description: "US CPI Release"
    impact: high
  - datetime: "2024-01-16 13:00:00"
    description: "FOMC Minutes"
    impact: high

# Manual suspensions
suspensions:
  - start: "2024-01-20 00:00:00"
    end: "2024-01-20 23:59:59"
    reason: "Holiday"

# Market close time
market_close_time: "16:55"  # 4:55 PM
```

## Usage Examples

### Basic Trading Setup

```python
from app.trader.executor_builder import ExecutorBuilder
from app.utils.config import LoadEnvironmentVariables

# Load configuration
config = LoadEnvironmentVariables(".env")

# Build trade executor
trade_executor = ExecutorBuilder.build_from_config(
    config=config,
    client=mt5_client,
    logger=logger
)

# Ready for trading cycle
```

### Complete Trading Loop

```python
from datetime import datetime
import time

# Main trading loop
while True:
    try:
        # 1. Fetch streaming data
        stream_data = data_manager.get_stream_data("EURUSD", "1", nbr_bars=3)

        # 2. Update indicators and regime
        new_bar = stream_data.iloc[-1]
        regime_data = regime_manager.update('1', new_bar)
        enriched_row = indicator_processor.process_new_row('1', new_bar, regime_data)

        # 3. Evaluate strategies
        recent_rows = indicator_processor.get_recent_rows()
        strategy_results = strategy_engine.evaluate(recent_rows)

        # 4. Generate entry decisions
        entry_decisions = []
        for name, result in strategy_results.strategies.items():
            if result.entry.long:
                entry = entry_manager.calculate_entry_decision(
                    strategy_name=name,
                    symbol="EURUSD",
                    direction="long",
                    entry_price=enriched_row['close'],
                    decision_time=datetime.now(),
                    market_data=recent_rows,
                    account_balance=account_balance
                )
                entry_decisions.append(entry)

        # 5. Execute trading cycle
        context = trade_executor.execute_trading_cycle(
            trades=entry_decisions,
            date_helper=date_helper
        )

        # 6. Log status
        logger.info(
            f"Cycle complete | "
            f"Can trade: {context.can_trade()} | "
            f"Daily P&L: ${context.daily_pnl:.2f} | "
            f"Positions: {len(context.open_positions)}"
        )

        # 7. Sleep
        time.sleep(5)

    except Exception as e:
        logger.error(f"Trading loop error: {e}")
        time.sleep(10)
```

### Manual Trade Execution

```python
# Create trader directly
from app.trader.live_trader import LiveTrader

trader = LiveTrader(client=mt5_client, logger=logger)

# Place market order
result = trader.create_market_order(
    symbol="EURUSD",
    direction="long",
    volume=0.1,
    stop_loss=1.0850,
    take_profit=1.0950,
    magic_number=123456,
    comment="Manual entry"
)

if result.get('success'):
    print(f"Order placed: {result['ticket']}")
else:
    print(f"Order failed: {result['error']}")
```

### Position Management

```python
# Get all open positions
positions = trader.get_open_positions()

for position in positions:
    print(f"Ticket: {position['ticket']}")
    print(f"Symbol: {position['symbol']}")
    print(f"Type: {position['type']}")
    print(f"Volume: {position['volume']}")
    print(f"Profit: {position['profit']}")

# Close specific position
trader.close_position(ticket=12345678)

# Modify position SL/TP
trader.modify_position(
    ticket=12345678,
    stop_loss=1.0860,
    take_profit=1.0960
)
```

## Order Splitting and Scaling

### Equal Splitting

```python
# Configuration
position_split = 3
scaling_type = "equal"
entry_spacing = 0.0005  # 5 pips

# Entry decision: 0.1 lots at 1.0900
# Creates 3 equal orders:
# 1. 0.033 lots at 1.0900 (current price)
# 2. 0.033 lots at 1.0895 (5 pips lower)
# 3. 0.033 lots at 1.0890 (10 pips lower)
```

### Pyramid Scaling

```python
# Configuration
position_split = 3
scaling_type = "pyramid"
entry_spacing = 0.0005  # 5 pips

# Entry decision: 0.1 lots at 1.0900
# Creates pyramid orders:
# 1. 0.05 lots at 1.0900 (50% - current price)
# 2. 0.03 lots at 1.0895 (30% - 5 pips lower)
# 3. 0.02 lots at 1.0890 (20% - 10 pips lower)
```

## Risk Management

### Daily Loss Limit

```python
# Set daily loss limit
DAILY_LOSS_LIMIT = 1000.0  # $1000

# Risk monitor tracks P&L
daily_pnl = risk_monitor.calculate_daily_pnl(positions, history)

if daily_pnl <= -DAILY_LOSS_LIMIT:
    # Loss limit breached
    logger.error("Daily loss limit breached - stopping trading")
    context.update_risk_breach(breached=True)
    # No new trades will be executed
```

### Position Limits

```python
# Maximum concurrent positions
MAX_POSITIONS = 10

# Check before entry
if len(open_positions) >= MAX_POSITIONS:
    logger.warning("Maximum positions reached")
    # Skip entry
```

### Risk Per Trade

```python
# Configured in entry manager
risk:
  sl:
    type: monetary
    value: 500.0  # $500 max loss per trade

# Entry manager calculates position size to match risk
```

## Trading Restrictions

### News-Based Restrictions

```yaml
# config/restrictions/eurusd.yaml
news_events:
  - datetime: "2024-01-15 14:30:00"
    description: "US CPI Release"
    impact: high

# Trading blocked 5 minutes before and after news
# Block period: 14:25 - 14:35
```

### Market Close Restrictions

```yaml
# config/restrictions/eurusd.yaml
market_close_time: "16:55"

# Trading blocked 5 minutes before close
# Block period: 16:50 - 16:55
```

### Manual Suspensions

```yaml
# config/restrictions/eurusd.yaml
suspensions:
  - start: "2024-01-20 00:00:00"
    end: "2024-01-20 23:59:59"
    reason: "Holiday - markets closed"
```

## Integration Points

### With Entry Manager

```python
# Entry manager generates trade decisions
entry_decision = entry_manager.calculate_entry_decision(...)

# Trader executes decisions
context = trade_executor.execute_trading_cycle(
    trades=[entry_decision],
    date_helper=date_helper
)
```

### With Strategy Builder

```python
# Strategy builder provides exit signals
exit_decisions = []

for name, result in strategy_results.strategies.items():
    if result.exit.long:
        exit_decision = entry_manager.calculate_exit_decision(
            strategy_name=name,
            symbol="EURUSD",
            direction="long",
            decision_time=datetime.now()
        )
        exit_decisions.append(exit_decision)

# Exit manager processes exits
exit_manager.process_exits(exit_decisions, open_positions)
```

### With Clients Package

```python
# LiveTrader wraps MT5Client
from app.clients.mt5.client import create_client_with_retry

client = create_client_with_retry("http://localhost:8000")

trader = LiveTrader(client=client, logger=logger)

# All MT5 operations go through client
```

## Best Practices

### 1. Always Use TradeExecutor

```python
# Preferred: Use trade executor for complete cycle
context = trade_executor.execute_trading_cycle(trades, date_helper)

# Avoid: Direct trader calls (bypasses risk checks)
# trader.create_market_order(...)  # Don't do this in production
```

### 2. Monitor Trading Context

```python
# Check context after each cycle
if not context.can_trade():
    logger.warning(f"Trading blocked: {context.block_reasons}")

    # Take appropriate action
    if context.risk_breached:
        # Stop trading for the day
        break
    elif "news" in context.block_reasons[0].lower():
        # Wait for news event to pass
        time.sleep(60)
```

### 3. Log All Trading Activity

```python
# Log before execution
logger.info(f"Executing {len(entry_decisions)} entries")

# Log results
logger.info(
    f"Cycle complete | "
    f"Authorized: {context.can_trade()} | "
    f"P&L: ${context.daily_pnl:.2f} | "
    f"Positions: {len(context.open_positions)} | "
    f"Orders: {len(context.pending_orders)}"
)
```

### 4. Handle Errors Gracefully

```python
try:
    context = trade_executor.execute_trading_cycle(trades, date_helper)
except Exception as e:
    logger.error(f"Trading cycle failed: {e}")
    # Don't crash - continue to next cycle
    time.sleep(10)
    continue
```

### 5. Respect Trading Restrictions

```python
# Check restrictions before trading
is_restricted, reason = restriction_manager.is_restricted(
    current_time=datetime.now(),
    symbol="EURUSD"
)

if is_restricted:
    logger.info(f"Trading restricted: {reason}")
    # Skip this cycle
    continue
```

## Troubleshooting

### Orders Not Executing

**Issue**: Orders fail to execute
**Solutions**:
1. Check MT5 connection
2. Verify account has sufficient balance
3. Check position size meets broker minimums
4. Validate SL/TP levels are reasonable
5. Review error messages in logs

### Risk Limit Constantly Breached

**Issue**: Daily loss limit hit frequently
**Solutions**:
1. Reduce position sizes
2. Tighten stop losses
3. Increase daily loss limit (if appropriate)
4. Review strategy performance
5. Check for system errors causing losses

### Duplicate Positions

**Issue**: Multiple positions for same strategy
**Solutions**:
1. Verify duplicate filter is working
2. Check magic number uniqueness
3. Review strategy evaluation logic
4. Ensure proper position tracking

## Performance Considerations

1. **Execution Speed**: Complete trading cycle ~50-100ms
2. **API Calls**: Minimize calls to MT5 API
3. **Error Handling**: Graceful degradation on failures
4. **Logging**: Appropriate log levels (INFO for production)

## Testing

```python
import pytest
from app.trader.trade_executor import TradeExecutor

def test_trading_cycle():
    """Test complete trading cycle."""

    # Mock components
    mock_trader = Mock(spec=LiveTrader)
    mock_trader.get_open_positions.return_value = []
    mock_trader.get_pending_orders.return_value = []

    # Create executor
    executor = TradeExecutor(
        trader=mock_trader,
        exit_manager=mock_exit_manager,
        duplicate_filter=mock_filter,
        risk_monitor=mock_risk,
        order_executor=mock_order_exec,
        restriction_manager=mock_restrictions,
        logger=logger
    )

    # Execute cycle
    context = executor.execute_trading_cycle([], date_helper)

    # Assertions
    assert context.can_trade() == True
    assert context.daily_pnl == 0.0
```

## Conclusion

The trader package provides robust, production-ready trade execution with comprehensive risk management and safety features. Its modular design and clear separation of concerns make it maintainable and testable while ensuring reliable trading operations.
