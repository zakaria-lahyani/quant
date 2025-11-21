"""
Utility script to view manual signal queue and history in Redis.

This script displays pending signals, processing history, and queue statistics
for the ManualSignalService.

Usage:
    python scripts/view_manual_signals.py
    python scripts/view_manual_signals.py --account acg_daily
    python scripts/view_manual_signals.py --history 50
    python scripts/view_manual_signals.py --watch
"""

import argparse
import json
import sys
import time
from datetime import datetime
from typing import Optional

try:
    import redis
    from tabulate import tabulate
except ImportError as e:
    print(f"Error: Missing dependency - {e}")
    print("Install with: pip install redis tabulate")
    sys.exit(1)

# Add parent directory to path for imports
sys.path.insert(0, str(__file__).rsplit('scripts', 1)[0])

from app.infrastructure.events.manual_signal_store import ManualSignalStore, ManualSignal


def connect_redis(host: str = "localhost", port: int = 6379, db: int = 1) -> redis.Redis:
    """Connect to Redis server."""
    try:
        client = redis.Redis(host=host, port=port, db=db, decode_responses=False)
        client.ping()
        print(f"Connected to Redis at {host}:{port} (DB {db})")
        return client
    except Exception as e:
        print(f"Failed to connect to Redis: {e}")
        sys.exit(1)


def display_queue_status(client: redis.Redis, account: str) -> None:
    """Display pending signals in queue."""
    store = ManualSignalStore(redis_client=client, account_name=account)

    signals = store.peek_signals(limit=50)
    queue_length = store.get_queue_length()

    print(f"\n{'='*70}")
    print(f"  Pending Signals Queue [{account}]")
    print(f"{'='*70}")

    if not signals:
        print("\n  No pending signals in queue")
        print(f"\n  Queue length: 0")
        return

    table_data = []
    for i, signal in enumerate(signals, 1):
        action = getattr(signal, 'action', 'ENTRY')
        table_data.append([
            i,
            signal.symbol,
            action,
            signal.direction,
            signal.source,
            signal.entry_price or "Market",
            signal.timestamp[:19] if signal.timestamp else "-"
        ])

    print()
    print(tabulate(
        table_data,
        headers=['#', 'Symbol', 'Action', 'Direction', 'Source', 'Price', 'Timestamp'],
        tablefmt='grid'
    ))
    print(f"\n  Total pending: {queue_length}")


def display_history(client: redis.Redis, account: str, limit: int = 20) -> None:
    """Display signal processing history."""
    store = ManualSignalStore(redis_client=client, account_name=account)

    history = store.get_history(limit=limit)

    print(f"\n{'='*70}")
    print(f"  Signal Processing History [{account}] (last {limit})")
    print(f"{'='*70}")

    if not history:
        print("\n  No signal history")
        return

    table_data = []
    for entry in history:
        result = entry.get('result', {})
        status = result.get('status', 'unknown')
        status_icon = 'OK' if status == 'published' else 'FAIL'
        action = entry.get('action', 'ENTRY')

        table_data.append([
            status_icon,
            entry.get('symbol', '-'),
            action,
            entry.get('direction', '-'),
            entry.get('source', '-'),
            entry.get('timestamp', '-')[:19] if entry.get('timestamp') else '-',
            entry.get('processed_at', '-')[:19] if entry.get('processed_at') else '-',
            result.get('correlation_id', result.get('error', '-'))[:20]
        ])

    print()
    print(tabulate(
        table_data,
        headers=['Status', 'Symbol', 'Action', 'Dir', 'Source', 'Submitted', 'Processed', 'Result'],
        tablefmt='grid'
    ))


def display_stats(client: redis.Redis, account: str) -> None:
    """Display queue statistics."""
    store = ManualSignalStore(redis_client=client, account_name=account)

    queue_length = store.get_queue_length()
    history = store.get_history(limit=100)

    # Calculate stats
    success_count = sum(1 for h in history if h.get('result', {}).get('status') == 'published')
    fail_count = sum(1 for h in history if h.get('result', {}).get('status') == 'failed')
    total = len(history)

    print(f"\n{'='*70}")
    print(f"  Queue Statistics [{account}]")
    print(f"{'='*70}")

    stats = [
        ['Pending Signals', queue_length],
        ['Total Processed (last 100)', total],
        ['Successful', success_count],
        ['Failed', fail_count],
        ['Success Rate', f"{(success_count/total*100):.1f}%" if total > 0 else "N/A"],
    ]

    print()
    print(tabulate(stats, tablefmt='grid'))


def watch_queue(client: redis.Redis, account: str, interval: int = 2) -> None:
    """Watch queue in real-time."""
    print(f"\nWatching manual signal queue for [{account}] (Ctrl+C to stop)...")
    print()

    try:
        while True:
            # Clear screen
            print("\033[2J\033[H", end="")

            print(f"Manual Signal Queue Monitor - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Account: {account}")
            print()

            display_stats(client, account)
            display_queue_status(client, account)

            time.sleep(interval)

    except KeyboardInterrupt:
        print("\n\nStopped watching")


def main():
    parser = argparse.ArgumentParser(
        description='View manual signal queue and history',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    # Connection args
    parser.add_argument('--host', default='localhost', help='Redis host')
    parser.add_argument('--port', type=int, default=6379, help='Redis port')
    parser.add_argument('--db', type=int, default=1, help='Redis database')
    parser.add_argument('--account', default='acg_daily', help='Account name')

    # Display options
    parser.add_argument('--history', '-H', type=int, default=20,
                        help='Number of history entries to show')
    parser.add_argument('--watch', '-w', action='store_true',
                        help='Watch queue in real-time')
    parser.add_argument('--stats-only', '-s', action='store_true',
                        help='Show only statistics')

    args = parser.parse_args()

    client = connect_redis(args.host, args.port, args.db)

    if args.watch:
        watch_queue(client, args.account)
    elif args.stats_only:
        display_stats(client, args.account)
    else:
        display_stats(client, args.account)
        display_queue_status(client, args.account)
        display_history(client, args.account, args.history)


if __name__ == "__main__":
    main()
