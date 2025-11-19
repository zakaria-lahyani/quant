# Data Package

## Overview

The **data** package provides an abstraction layer for market data access, supporting both live trading (via MT5 API) and backtesting (via parquet files). It implements the Strategy pattern to enable seamless switching between data sources while maintaining a consistent interface.

## Main Features

- **Unified Data Interface**: Same API for live trading and backtesting
- **Multiple Data Sources**: Live (MT5) and historical (parquet files)
- **Streaming Simulation**: Backtest data source simulates real-time streaming
- **Date Range Filtering**: Smart date filtering for historical data
- **Factory Pattern**: Easy data source creation and management
- **Type Safety**: Full type hints and validation

## Package Structure

```
data/
├── data_interface.py      # Abstract interface defining data contract
├── live_data.py          # Live data source using MT5 API
├── backtest_data.py      # Backtest data source using parquet files
└── data_manager.py       # Factory and facade for data source management
```

## Key Components

### DataSourceInterface

Abstract base class defining the contract for all data sources:

```python
from abc import ABC, abstractmethod

class DataSourceInterface(ABC):
    @abstractmethod
    def get_historical_data(self, symbol: str, timeframe: str) -> pd.DataFrame:
        """Fetch historical data for warmup and initialization."""
        pass

    @abstractmethod
    def get_stream_data(self, symbol: str, timeframe: str, nbr_bars: int) -> pd.DataFrame:
        """Fetch streaming data (latest N bars)."""
        pass
```

### LiveDataSource

Real-time data fetching from MT5 API:
- Connects to MT5 through HTTP API client
- Handles date range calculations
- Filters data based on current date
- Suitable for live trading environments

### BacktestDataSource

Historical data from parquet files:
- Loads pre-downloaded historical data
- Simulates streaming by maintaining current position
- Supports incremental data fetching
- Perfect for strategy backtesting

### DataSourceManager

Factory and facade for unified data access:
- Creates appropriate data source based on mode
- Provides consistent interface regardless of source
- Simplifies dependency injection

## Usage Examples

### Live Trading Mode

```python
from app.data.data_manager import DataSourceManager
from app.clients.mt5.client import create_client_with_retry
from app.utils.date_helper import DateHelper

# Create MT5 client
client = create_client_with_retry("http://localhost:8000")

# Create date helper
date_helper = DateHelper()

# Create data manager for live trading
data_manager = DataSourceManager(
    mode="live",
    client=client,
    date_helper=date_helper
)

# Fetch historical data for warmup (500 bars)
historical = data_manager.get_historical_data(
    symbol="EURUSD",
    timeframe="1",  # 1-minute
    nbr_bars=500
)

# Fetch latest streaming data (last 3 bars)
stream = data_manager.get_stream_data(
    symbol="EURUSD",
    timeframe="1",
    nbr_bars=3
)
```

### Backtesting Mode

```python
from app.data.data_manager import DataSourceManager

# Create data manager for backtesting
data_manager = DataSourceManager(
    mode="backtest",
    data_path="./data",  # Folder containing parquet files
    symbol="eurusd"      # Symbol name (lowercase for file matching)
)

# Load historical data (all available data)
historical = data_manager.get_historical_data(
    symbol="eurusd",
    timeframe="1"
)

# Simulate streaming (get next 3 bars)
stream = data_manager.get_stream_data(
    symbol="eurusd",
    timeframe="1",
    nbr_bars=3
)

# Continue simulation (next 3 bars)
stream = data_manager.get_stream_data(
    symbol="eurusd",
    timeframe="1",
    nbr_bars=3
)
```

### Using Data Sources Directly

```python
from app.data.live_data import LiveDataSource
from app.data.backtest_data import BacktestDataSource

# Live data source
live_source = LiveDataSource(
    client=mt5_client,
    date_helper=date_helper
)

bars = live_source.get_historical_data("EURUSD", "1", nbr_bars=100)

# Backtest data source
backtest_source = BacktestDataSource(
    data_path="./data",
    symbol="eurusd"
)

bars = backtest_source.get_historical_data("eurusd", "1")
```

## Data Format

All data sources return pandas DataFrames with standardized columns:

```python
# Required columns
df.columns = ['time', 'open', 'high', 'low', 'close', 'volume']

# Data types
df['time'] = pd.Timestamp
df['open'] = float
df['high'] = float
df['low'] = float
df['close'] = float
df['volume'] = float

# Index
df.index = RangeIndex (not time-based)
```

Example:

```
        time                 open      high      low       close    volume
0       2024-01-01 00:00:00  1.0900    1.0905    1.0895    1.0902   1234
1       2024-01-01 00:01:00  1.0902    1.0910    1.0900    1.0908   1567
2       2024-01-01 00:02:00  1.0908    1.0915    1.0905    1.0912   1890
```

## Configuration

### Environment Variables

```bash
# Trading mode
TRADE_MODE=live  # or "backtest"

# Live mode configuration
API_BASE_URL=http://localhost:8000

# Backtest mode configuration
BACKTEST_DATA_PATH=./data
SYMBOL=EURUSD
```

### Data File Structure

For backtesting, organize parquet files as follows:

```
data/
├── eurusd_1.parquet    # 1-minute data
├── eurusd_5.parquet    # 5-minute data
├── eurusd_15.parquet   # 15-minute data
├── eurusd_30.parquet   # 30-minute data
└── eurusd_60.parquet   # 1-hour data
```

File naming convention: `{symbol}_{timeframe}.parquet`

## Best Practices

### 1. Use DataSourceManager

```python
# Preferred: Use manager for abstraction
data_manager = DataSourceManager(mode="live", client=client, date_helper=date_helper)

# Instead of: Direct instantiation
live_source = LiveDataSource(client, date_helper)
```

### 2. Handle Missing Data

```python
def safe_data_fetch(data_manager, symbol, timeframe):
    try:
        data = data_manager.get_historical_data(symbol, timeframe)

        if data is None or data.empty:
            logger.warning(f"No data received for {symbol} {timeframe}")
            return None

        return data

    except Exception as e:
        logger.error(f"Data fetch failed: {e}")
        return None
```

### 3. Validate Data Quality

```python
def validate_data(df: pd.DataFrame) -> bool:
    """Validate data frame has required structure."""

    # Check required columns
    required_cols = ['time', 'open', 'high', 'low', 'close', 'volume']
    if not all(col in df.columns for col in required_cols):
        return False

    # Check for NaN values
    if df[required_cols].isnull().any().any():
        return False

    # Check OHLC consistency
    if not (df['high'] >= df['low']).all():
        return False

    if not ((df['high'] >= df['open']) & (df['high'] >= df['close'])).all():
        return False

    if not ((df['low'] <= df['open']) & (df['low'] <= df['close'])).all():
        return False

    return True
```

### 4. Cache Historical Data

```python
class CachedDataManager:
    def __init__(self, data_manager):
        self.data_manager = data_manager
        self.cache = {}

    def get_historical_data(self, symbol, timeframe, nbr_bars=None):
        cache_key = f"{symbol}_{timeframe}_{nbr_bars}"

        if cache_key not in self.cache:
            self.cache[cache_key] = self.data_manager.get_historical_data(
                symbol, timeframe, nbr_bars
            )

        return self.cache[cache_key]
```

### 5. Environment-Based Configuration

```python
import os
from app.data.data_manager import DataSourceManager

def create_data_manager():
    """Create data manager based on environment."""
    mode = os.getenv("TRADE_MODE", "backtest")

    if mode == "live":
        from app.clients.mt5.client import create_client_with_retry
        from app.utils.date_helper import DateHelper

        client = create_client_with_retry(os.getenv("API_BASE_URL"))
        date_helper = DateHelper()

        return DataSourceManager(
            mode="live",
            client=client,
            date_helper=date_helper
        )
    else:
        return DataSourceManager(
            mode="backtest",
            data_path=os.getenv("BACKTEST_DATA_PATH", "./data"),
            symbol=os.getenv("SYMBOL", "eurusd").lower()
        )
```

## Integration Points

### With Indicators Package

```python
from app.indicators.indicator_processor import IndicatorProcessor

# Fetch historical data for indicator warmup
historicals = {
    '1': data_manager.get_historical_data("EURUSD", "1", nbr_bars=500),
    '5': data_manager.get_historical_data("EURUSD", "5", nbr_bars=500),
}

# Create indicator processor
indicator_processor = IndicatorProcessor(
    configs=configs,
    historicals=historicals,
    is_bulk=False
)
```

### With Regime Package

```python
from app.regime.regime_manager import RegimeManager

# Setup regime manager with historical data
regime_manager = RegimeManager(warmup_bars=500)

historicals = {
    '1': data_manager.get_historical_data("EURUSD", "1"),
    '5': data_manager.get_historical_data("EURUSD", "5"),
}

regime_manager.setup(
    timeframes=['1', '5'],
    historicals=historicals
)
```

### With Main Trading Loop

```python
# main_live_regime.py integration
from app.data.data_manager import DataSourceManager

# Initialize data manager
data_manager = DataSourceManager(
    mode=config.trade_mode,
    client=mt5_client,
    date_helper=date_helper
)

# Trading loop
while True:
    # Fetch streaming data
    stream_data = {
        '1': data_manager.get_stream_data("EURUSD", "1", nbr_bars=3),
        '5': data_manager.get_stream_data("EURUSD", "5", nbr_bars=3),
    }

    # Process data...
```

## Advanced Features

### Multiple Timeframes

```python
# Fetch data for multiple timeframes
timeframes = ['1', '5', '15', '60']
data = {}

for tf in timeframes:
    data[tf] = data_manager.get_historical_data(
        symbol="EURUSD",
        timeframe=tf,
        nbr_bars=500
    )
```

### Date Range Filtering

```python
from datetime import datetime, timedelta

# Live data automatically filters by date
# Get data from last 30 days
data_manager = DataSourceManager(
    mode="live",
    client=client,
    date_helper=date_helper
)

# DateHelper calculates: 30 days ago to today
historical = data_manager.get_historical_data("EURUSD", "1")
```

### Streaming Simulation (Backtest)

```python
# Backtest data source maintains current position
backtest_source = BacktestDataSource("./data", "eurusd")

# First call - returns bars 0-2
batch_1 = backtest_source.get_stream_data("eurusd", "1", nbr_bars=3)

# Second call - returns bars 3-5
batch_2 = backtest_source.get_stream_data("eurusd", "1", nbr_bars=3)

# Third call - returns bars 6-8
batch_3 = backtest_source.get_stream_data("eurusd", "1", nbr_bars=3)
```

## Troubleshooting

### No Data Returned

```python
# Check connection (live mode)
try:
    account = client.account.get_account_info()
    logger.info("MT5 API connected")
except Exception as e:
    logger.error(f"Cannot connect to MT5: {e}")

# Check file exists (backtest mode)
import os
data_file = f"./data/{symbol}_{timeframe}.parquet"
if not os.path.exists(data_file):
    logger.error(f"Data file not found: {data_file}")
```

### Invalid Timeframe

```python
# Valid timeframes for MT5
VALID_TIMEFRAMES = ['1', '5', '15', '30', '60', '240', 'D1', 'W1', 'MN1']

def validate_timeframe(timeframe: str) -> bool:
    return timeframe in VALID_TIMEFRAMES
```

### Data Quality Issues

```python
# Check data after fetching
data = data_manager.get_historical_data("EURUSD", "1")

# Verify not empty
assert not data.empty, "Data is empty"

# Verify columns
required = ['time', 'open', 'high', 'low', 'close', 'volume']
assert all(col in data.columns for col in required), "Missing columns"

# Verify no NaN
assert not data.isnull().any().any(), "Data contains NaN values"

# Verify OHLC logic
assert (data['high'] >= data['low']).all(), "High < Low detected"
assert (data['high'] >= data['open']).all(), "High < Open detected"
assert (data['high'] >= data['close']).all(), "High < Close detected"
```

## Testing

### Mock Data Source

```python
from unittest.mock import Mock
import pandas as pd

def create_mock_data_source():
    mock_source = Mock(spec=DataSourceInterface)

    # Mock historical data
    mock_source.get_historical_data.return_value = pd.DataFrame({
        'time': pd.date_range('2024-01-01', periods=100, freq='1min'),
        'open': [1.09 + i * 0.0001 for i in range(100)],
        'high': [1.09 + i * 0.0001 + 0.0005 for i in range(100)],
        'low': [1.09 + i * 0.0001 - 0.0005 for i in range(100)],
        'close': [1.09 + i * 0.0001 + 0.0002 for i in range(100)],
        'volume': [1000 for _ in range(100)]
    })

    return mock_source
```

### Integration Test

```python
def test_data_manager_switching():
    """Test switching between live and backtest modes."""

    # Test live mode
    live_manager = DataSourceManager(
        mode="live",
        client=mock_client,
        date_helper=date_helper
    )
    live_data = live_manager.get_historical_data("EURUSD", "1")
    assert isinstance(live_data, pd.DataFrame)

    # Test backtest mode
    backtest_manager = DataSourceManager(
        mode="backtest",
        data_path="./test_data",
        symbol="eurusd"
    )
    backtest_data = backtest_manager.get_historical_data("eurusd", "1")
    assert isinstance(backtest_data, pd.DataFrame)
```

## Performance Considerations

1. **Caching**: Cache historical data to avoid repeated API calls
2. **Batch Size**: Use appropriate `nbr_bars` for streaming data
3. **Date Filtering**: Live data filters by date range automatically
4. **Memory**: Backtest loads entire datasets into memory
5. **API Limits**: Respect MT5 API rate limits in live mode

## Migration Guide

### From Direct API Calls to Data Manager

Before:
```python
from app.clients.mt5.client import MT5Client

client = MT5Client("http://localhost:8000")
bars = client.data.fetch_bars("EURUSD", "1", num_bars=100)
```

After:
```python
from app.data.data_manager import DataSourceManager

data_manager = DataSourceManager(mode="live", client=client, date_helper=date_helper)
bars = data_manager.get_historical_data("EURUSD", "1", nbr_bars=100)
```

Benefits:
- Easy switching between live and backtest
- Consistent interface
- Better testability
- Date filtering handled automatically
