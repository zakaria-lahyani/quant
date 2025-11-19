# Clients Package

## Overview

The **clients** package provides a robust HTTP-based API client for interacting with MetaTrader 5 (MT5) trading platform through a REST API. It abstracts MT5 operations into clean, pythonic interfaces with comprehensive error handling and retry logic.

## Main Features

- **Unified MT5 Interface**: Single client providing access to all MT5 functionality
- **HTTP-based Communication**: RESTful API communication with JSON serialization
- **Comprehensive Error Handling**: Custom exception hierarchy for different error types
- **Retry Logic**: Built-in retry mechanism with configurable backoff
- **Type Safety**: Full type hints and model validation
- **Modular Design**: Specialized clients for different MT5 domains

## Package Structure

```
clients/
├── mt5/
│   ├── client.py          # Main MT5Client facade and factory functions
│   ├── base.py            # BaseClient with HTTP communication
│   ├── exceptions.py      # Custom exception hierarchy
│   ├── api/
│   │   ├── data.py        # Historical and real-time market data
│   │   ├── positions.py   # Position management
│   │   ├── orders.py      # Order operations
│   │   ├── account.py     # Account information
│   │   ├── history.py     # Historical trades
│   │   └── symbols.py     # Symbol information
│   └── models/
│       └── ...            # Data models for requests/responses
```

## Key Components

### MT5Client

The main facade providing unified access to all MT5 functionality:

```python
from app.clients.mt5.client import MT5Client

client = MT5Client(base_url="http://localhost:8000")

# Access specialized clients
client.data        # DataClient
client.positions   # PositionsClient
client.orders      # OrdersClient
client.account     # AccountClient
client.history     # HistoryClient
client.symbols     # SymbolsClient
```

### BaseClient

Foundation class handling all HTTP communication:

- Request/response lifecycle management
- JSON parsing and error handling
- Retry logic with exponential backoff
- Timeout configuration
- Session management

### Specialized API Clients

#### DataClient
Fetches historical and real-time market data:
- Get latest bars for a symbol
- Fetch historical OHLC data with date ranges
- Support for multiple timeframes

#### PositionsClient
Manages open positions:
- Get all open positions
- Get positions for specific symbol
- Modify position (SL/TP)
- Close positions (full or partial)

#### OrdersClient
Handles order operations:
- Create market orders (buy/sell)
- Create pending orders (limit, stop, stop-limit)
- Cancel orders (single or bulk)
- Modify existing orders

#### AccountClient
Retrieves account information:
- Account balance and equity
- Margin information
- Account currency and leverage

#### HistoryClient
Accesses historical trade data:
- Get historical deals
- Get historical orders
- Filter by date range

#### SymbolsClient
Provides symbol information:
- Get symbol details
- Market status
- Trading specifications

## Usage Examples

### Basic Setup

```python
from app.clients.mt5.client import create_client_with_retry

# Create client with automatic retry
client = create_client_with_retry(
    base_url="http://localhost:8000",
    max_retries=3,
    timeout=30
)
```

### Fetching Market Data

```python
# Get latest 100 H1 bars
bars = client.data.fetch_bars(
    symbol="EURUSD",
    timeframe="H1",
    num_bars=100
)

# Get data for date range
from datetime import datetime, timedelta

end_date = datetime.now()
start_date = end_date - timedelta(days=30)

bars = client.data.fetch_bars(
    symbol="EURUSD",
    timeframe="H1",
    start_date=start_date,
    end_date=end_date
)
```

### Creating Orders

```python
# Market buy order
result = client.orders.create_buy_order(
    symbol="EURUSD",
    volume=0.1,
    stop_loss=1.0850,
    take_profit=1.0950,
    comment="Strategy ABC"
)

# Limit order
result = client.orders.create_limit_order(
    symbol="EURUSD",
    order_type="buy_limit",
    volume=0.1,
    price=1.0900,
    stop_loss=1.0850,
    take_profit=1.0950
)
```

### Managing Positions

```python
# Get all open positions
positions = client.positions.get_open_positions()

# Get positions for specific symbol
eurusd_positions = client.positions.get_positions_by_symbol("EURUSD")

# Modify position SL/TP
client.positions.modify_position(
    ticket=12345678,
    stop_loss=1.0850,
    take_profit=1.0950
)

# Close position
client.positions.close_position(
    ticket=12345678,
    volume=0.1  # Partial close, omit for full close
)
```

### Account Information

```python
# Get account details
account = client.account.get_account_info()

print(f"Balance: {account['balance']}")
print(f"Equity: {account['equity']}")
print(f"Margin Free: {account['margin_free']}")
```

## Error Handling

The package provides a comprehensive exception hierarchy:

```python
from app.clients.mt5.exceptions import (
    APIError,           # Base exception
    ValidationError,    # Invalid parameters
    ConnectionError,    # Connection issues
    TimeoutError,       # Request timeout
    AuthenticationError,# Auth failures
    RateLimitError      # Rate limiting
)

try:
    result = client.orders.create_buy_order("EURUSD", 0.1)
except ValidationError as e:
    print(f"Invalid parameters: {e}")
except ConnectionError as e:
    print(f"Connection failed: {e}")
except APIError as e:
    print(f"API error: {e}")
```

## Configuration

### Environment Variables

```bash
# Required
API_BASE_URL=http://localhost:8000

# Optional
API_TIMEOUT=30  # Request timeout in seconds
```

### Client Configuration

```python
client = MT5Client(
    base_url="http://localhost:8000",
    timeout=30,
    max_retries=3,
    retry_backoff=1.0
)
```

## Best Practices

### 1. Use Factory Functions

```python
# Preferred: Automatic retry logic
client = create_client_with_retry("http://localhost:8000")

# Instead of: Manual instantiation
client = MT5Client("http://localhost:8000")
```

### 2. Handle Errors Gracefully

```python
from app.clients.mt5.exceptions import APIError

def safe_order_creation(symbol, volume):
    try:
        return client.orders.create_buy_order(symbol, volume)
    except ValidationError:
        logger.error(f"Invalid order parameters for {symbol}")
        return None
    except ConnectionError:
        logger.error("MT5 API unavailable")
        return None
    except APIError as e:
        logger.error(f"Order failed: {e}")
        return None
```

### 3. Check Response Status

```python
result = client.orders.create_buy_order("EURUSD", 0.1)

if result.get('success'):
    ticket = result.get('ticket')
    print(f"Order created: {ticket}")
else:
    error = result.get('error')
    print(f"Order failed: {error}")
```

### 4. Use Context Managers for Cleanup

```python
# If implementing session management
with MT5Client("http://localhost:8000") as client:
    positions = client.positions.get_open_positions()
    # Client automatically cleaned up
```

### 5. Implement Timeouts

```python
# Always set reasonable timeouts
client = MT5Client(
    base_url="http://localhost:8000",
    timeout=30  # Prevent hanging requests
)
```

## Integration Points

### With Data Package
```python
# LiveDataSource uses MT5Client for data fetching
from app.data.live_data import LiveDataSource

data_source = LiveDataSource(client=client, date_helper=date_helper)
bars = data_source.get_historical_data("EURUSD", "H1")
```

### With Trader Package
```python
# LiveTrader wraps MT5Client for trading operations
from app.trader.live_trader import LiveTrader

trader = LiveTrader(client=client, logger=logger)
trader.create_market_order("EURUSD", "buy", 0.1, 1.0850, 1.0950)
```

## Advanced Features

### Retry Logic

The client includes automatic retry with exponential backoff:

```python
client = create_client_with_retry(
    base_url="http://localhost:8000",
    max_retries=3,      # Retry up to 3 times
    retry_backoff=1.0   # Start with 1s, exponential increase
)
```

### Bulk Operations

```python
# Cancel multiple orders at once
order_tickets = [12345, 12346, 12347]
client.orders.cancel_orders(order_tickets)
```

### Symbol Information

```python
# Get detailed symbol info
symbol_info = client.symbols.get_symbol_info("EURUSD")

print(f"Digits: {symbol_info['digits']}")
print(f"Point: {symbol_info['point']}")
print(f"Min Volume: {symbol_info['volume_min']}")
print(f"Max Volume: {symbol_info['volume_max']}")
```

## Troubleshooting

### Connection Issues

```python
# Check if API is reachable
try:
    account = client.account.get_account_info()
    print("Connection OK")
except ConnectionError:
    print("Cannot connect to MT5 API")
```

### Validation Errors

```python
# Common validation issues:
# - Invalid symbol name
# - Invalid volume (must be in steps of volume_step)
# - Invalid timeframe
# - Missing required parameters

# Always validate inputs before API calls
def validate_volume(symbol, volume):
    symbol_info = client.symbols.get_symbol_info(symbol)
    min_vol = symbol_info['volume_min']
    max_vol = symbol_info['volume_max']
    step = symbol_info['volume_step']

    if volume < min_vol or volume > max_vol:
        raise ValueError(f"Volume must be between {min_vol} and {max_vol}")

    if (volume / step) % 1 != 0:
        raise ValueError(f"Volume must be multiple of {step}")

    return True
```

## Testing

```python
# Mock client for testing
from unittest.mock import Mock

mock_client = Mock(spec=MT5Client)
mock_client.orders.create_buy_order.return_value = {
    'success': True,
    'ticket': 12345678
}

# Use in tests
result = mock_client.orders.create_buy_order("EURUSD", 0.1)
assert result['success'] == True
```

## Performance Considerations

1. **Connection Pooling**: BaseClient maintains session for connection reuse
2. **Timeouts**: Always configure appropriate timeouts to prevent hanging
3. **Bulk Operations**: Use bulk methods when operating on multiple items
4. **Caching**: Consider caching symbol information (changes rarely)
5. **Rate Limiting**: Respect API rate limits to avoid RateLimitError

## Security

1. **HTTPS**: Always use HTTPS in production
2. **Authentication**: Implement proper authentication if required
3. **Secrets Management**: Never hardcode API URLs or credentials
4. **Input Validation**: Validate all inputs before sending to API
5. **Error Exposure**: Don't expose internal errors to end users
