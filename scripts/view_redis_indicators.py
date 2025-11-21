"""
Utility script to view indicator data stored in Redis.

This script connects to Redis and displays the latest computed indicators
and recent rows for all symbols and timeframes.

Usage:
    python scripts/view_redis_indicators.py
    python scripts/view_redis_indicators.py --symbol BTCUSD
    python scripts/view_redis_indicators.py --symbol BTCUSD --timeframe 15
    python scripts/view_redis_indicators.py --recent 10
"""

import argparse
import json
import redis
from typing import Optional, List, Dict, Any
from datetime import datetime
from tabulate import tabulate


ACCOUNT_META_KEY = "__account_meta__"


def connect_redis(host: str = "localhost", port: int = 6379, db: int = 0) -> redis.Redis:
    """Connect to Redis server."""
    try:
        client = redis.Redis(host=host, port=port, db=db, decode_responses=False)
        client.ping()
        print(f"Connected to Redis at {host}:{port} (DB {db})")
        return client
    except Exception as e:
        print(f"Failed to connect to Redis: {e}")
        exit(1)


def get_account_info(client: redis.Redis) -> Dict[str, str]:
    """Get account metadata from this Redis DB."""
    try:
        data = client.hgetall(ACCOUNT_META_KEY)
        if data:
            return {
                k.decode('utf-8'): v.decode('utf-8')
                for k, v in data.items()
            }
    except Exception:
        pass
    return {'account_name': 'Unknown', 'account_tag': 'Unknown'}


def get_all_symbols(client: redis.Redis) -> List[str]:
    """Get list of all symbols with indicator data."""
    pattern = "indicators:*:*:latest"
    keys = list(client.scan_iter(match=pattern))

    symbols = set()
    for key in keys:
        key_str = key.decode('utf-8')
        parts = key_str.split(':')
        if len(parts) >= 2:
            symbols.add(parts[1])

    return sorted(list(symbols))


def get_timeframes_for_symbol(client: redis.Redis, symbol: str) -> List[str]:
    """Get list of timeframes for a symbol."""
    pattern = f"indicators:{symbol}:*:latest"
    keys = list(client.scan_iter(match=pattern))

    timeframes = set()
    for key in keys:
        key_str = key.decode('utf-8')
        parts = key_str.split(':')
        if len(parts) >= 3:
            timeframes.add(parts[2])

    return sorted(list(timeframes))


def get_latest_indicators(client: redis.Redis, symbol: str, timeframe: str) -> Optional[Dict[str, Any]]:
    """Get latest indicator values."""
    key = f"indicators:{symbol}:{timeframe}:latest"
    data = client.hgetall(key)

    if not data:
        return None

    result = {}
    for k, v in data.items():
        key_str = k.decode('utf-8')
        value_str = v.decode('utf-8')
        try:
            result[key_str] = json.loads(value_str)
        except json.JSONDecodeError:
            result[key_str] = value_str

    return result


def get_recent_rows(client: redis.Redis, symbol: str, timeframe: str, limit: int = 10) -> List[Dict[str, Any]]:
    """Get recent indicator rows."""
    key = f"indicators:{symbol}:{timeframe}:recent"
    rows_json = client.lrange(key, 0, limit - 1)

    rows = []
    for row_json in rows_json:
        try:
            row_dict = json.loads(row_json.decode('utf-8'))
            rows.append(row_dict)
        except json.JSONDecodeError:
            continue

    return rows


def get_metadata(client: redis.Redis, symbol: str, timeframe: str) -> Dict[str, Any]:
    """Get metadata about stored indicators."""
    key = f"indicators:{symbol}:{timeframe}:metadata"
    data = client.hgetall(key)

    return {
        k.decode('utf-8'): v.decode('utf-8')
        for k, v in data.items()
    }


def format_value(value: Any) -> str:
    """Format a value for display."""
    if value is None:
        return "None"
    elif isinstance(value, float):
        return f"{value:.4f}"
    elif isinstance(value, (int, str, bool)):
        return str(value)
    else:
        return str(value)[:50]  # Truncate long values


def display_latest_indicators(client: redis.Redis, symbol: str, timeframe: str):
    """Display latest indicators for a symbol/timeframe."""
    print(f"\n{'='*80}")
    print(f"Latest Indicators: {symbol} {timeframe}")
    print(f"{'='*80}")

    indicators = get_latest_indicators(client, symbol, timeframe)
    if not indicators:
        print("  No data available")
        return

    # Get metadata
    metadata = get_metadata(client, symbol, timeframe)
    if metadata:
        print(f"\nLast Update: {metadata.get('last_update', 'Unknown')}")
        print(f"Indicators: {metadata.get('num_indicators', 'Unknown')}")
        print(f"Has Regime: {metadata.get('has_regime', 'Unknown')}")

    # Display key indicators in a table
    important_fields = ['time', 'close', 'open', 'high', 'low', 'volume', 'regime', 'regime_confidence']
    table_data = []

    for field in important_fields:
        if field in indicators:
            table_data.append([field, format_value(indicators[field])])

    if table_data:
        print("\n" + tabulate(table_data, headers=['Field', 'Value'], tablefmt='grid'))

    # Display all other indicators
    other_fields = [k for k in sorted(indicators.keys()) if k not in important_fields]
    if other_fields:
        print(f"\nOther Indicators ({len(other_fields)}):")
        other_table = [[field, format_value(indicators[field])] for field in other_fields[:20]]
        print(tabulate(other_table, headers=['Field', 'Value'], tablefmt='simple'))

        if len(other_fields) > 20:
            print(f"  ... and {len(other_fields) - 20} more fields")


def display_recent_rows(client: redis.Redis, symbol: str, timeframe: str, limit: int = 10):
    """Display recent rows with indicators."""
    print(f"\n{'='*80}")
    print(f"Recent Rows: {symbol} {timeframe} (last {limit})")
    print(f"{'='*80}")

    rows = get_recent_rows(client, symbol, timeframe, limit)
    if not rows:
        print("  No data available")
        return

    print(f"\nFound {len(rows)} recent rows")

    # Display summary table
    fields_to_show = ['time', 'close', 'volume', 'regime']
    table_data = []

    for i, row in enumerate(rows):
        row_data = [f"Row {i+1}"]
        for field in fields_to_show:
            if field in row:
                row_data.append(format_value(row[field]))
            else:
                row_data.append("-")
        table_data.append(row_data)

    headers = ['#'] + fields_to_show
    print("\n" + tabulate(table_data, headers=headers, tablefmt='grid'))


def display_all_symbols(client: redis.Redis):
    """Display overview of all symbols and timeframes."""
    # Get account info first
    account_info = get_account_info(client)
    account_name = account_info.get('account_name', 'Unknown')
    account_tag = account_info.get('account_tag', 'Unknown')
    last_active = account_info.get('last_active', 'Unknown')

    print(f"\n{'='*80}")
    print(f"Account: {account_name} ({account_tag})")
    if last_active != 'Unknown':
        try:
            dt = datetime.fromisoformat(last_active)
            print(f"Last Active: {dt.strftime('%Y-%m-%d %H:%M:%S')}")
        except:
            print(f"Last Active: {last_active}")
    print(f"{'='*80}\n")

    symbols = get_all_symbols(client)
    if not symbols:
        print("No indicator data found in Redis")
        return

    table_data = []
    for symbol in symbols:
        timeframes = get_timeframes_for_symbol(client, symbol)

        # Get last update for each timeframe
        tf_info = []
        for tf in timeframes:
            metadata = get_metadata(client, symbol, tf)
            last_update = metadata.get('last_update', 'Unknown')
            if last_update != 'Unknown':
                try:
                    dt = datetime.fromisoformat(last_update)
                    last_update = dt.strftime('%H:%M:%S')
                except:
                    pass
            tf_info.append(f"{tf}({last_update})")

        table_data.append([
            symbol,
            len(timeframes),
            ", ".join(tf_info)
        ])

    print(tabulate(table_data, headers=['Symbol', 'Timeframes', 'Details'], tablefmt='grid'))


def main():
    parser = argparse.ArgumentParser(description='View indicator data stored in Redis')
    parser.add_argument('--host', default='localhost', help='Redis host (default: localhost)')
    parser.add_argument('--port', type=int, default=6379, help='Redis port (default: 6379)')
    parser.add_argument('--db', type=int, default=1, help='Redis database number (default: 0)')
    parser.add_argument('--symbol', help='Symbol to view (e.g., BTCUSD)')
    parser.add_argument('--timeframe', help='Timeframe to view (e.g., 15)')
    parser.add_argument('--recent', type=int, default=10, help='Number of recent rows to show (default: 10)')
    parser.add_argument('--list', action='store_true', help='List all symbols and timeframes')

    args = parser.parse_args()

    # Connect to Redis
    client = connect_redis(args.host, args.port, args.db)

    # Display overview if no specific symbol requested
    if args.list or (not args.symbol and not args.timeframe):
        display_all_symbols(client)
        return

    # Get symbol (or all symbols)
    if args.symbol:
        symbols = [args.symbol.upper()]
    else:
        symbols = get_all_symbols(client)

    # Display data for each symbol
    for symbol in symbols:
        # Get timeframes (or all timeframes)
        if args.timeframe:
            timeframes = [args.timeframe]
        else:
            timeframes = get_timeframes_for_symbol(client, symbol)

        if not timeframes:
            print(f"\nNo data found for {symbol}")
            continue

        # Display data for each timeframe
        for timeframe in timeframes:
            display_latest_indicators(client, symbol, timeframe)
            display_recent_rows(client, symbol, timeframe, args.recent)


if __name__ == "__main__":
    main()
