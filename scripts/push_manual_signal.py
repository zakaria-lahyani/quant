"""
Utility script to push manual trading signals to Redis.

This script allows external systems or manual traders to submit
trading signals that will be processed by the ManualSignalService.

The signal goes through the full risk management pipeline:
- Position sizing based on account balance
- Stop loss and take profit calculation
- Trade execution via broker

Usage:
    # Basic entry signal (default action is ENTRY)
    python scripts/push_manual_signal.py --symbol XAUUSD --direction BUY

    # Explicit entry signal
    python scripts/push_manual_signal.py --symbol XAUUSD --direction BUY --action ENTRY

    # Exit signal (close all positions)
    python scripts/push_manual_signal.py --symbol XAUUSD --direction BUY --action EXIT
    # Note: EXIT BUY = close long positions, EXIT SELL = close short positions

    # Partial exit (close 50% of positions)
    python scripts/push_manual_signal.py --symbol XAUUSD --direction BUY --action EXIT --close-percent 50

    # With source identifier
    python scripts/push_manual_signal.py --symbol XAUUSD --direction SELL --source telegram_bot

    # With specific entry price
    python scripts/push_manual_signal.py --symbol XAUUSD --direction BUY --price 2650.50

    # For a specific account
    python scripts/push_manual_signal.py --account acg_daily --symbol XAUUSD --direction BUY

    # View pending signals
    python scripts/push_manual_signal.py --list

    # View signal history
    python scripts/push_manual_signal.py --history

    # Clear pending signals
    python scripts/push_manual_signal.py --clear
"""

import argparse
import json
import sys
from datetime import datetime
from typing import Optional

try:
    import redis
except ImportError:
    print("Error: redis-py not installed. Run: pip install redis")
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


def push_signal(
    client: redis.Redis,
    account: str,
    symbol: str,
    direction: str,
    action: str = "ENTRY",
    source: str = "manual_cli",
    entry_price: Optional[float] = None,
    close_percent: Optional[float] = None,
    metadata: Optional[dict] = None
) -> bool:
    """
    Push a manual trading signal to Redis.

    Args:
        client: Redis client
        account: Account name (key prefix)
        symbol: Trading symbol (e.g., XAUUSD)
        direction: BUY or SELL
        action: ENTRY or EXIT (default: ENTRY)
        source: Signal source identifier
        entry_price: Optional entry price
        close_percent: Percentage of position to close (1-100, only for EXIT)
        metadata: Optional additional metadata

    Returns:
        True if signal was queued successfully
    """
    store = ManualSignalStore(
        redis_client=client,
        account_name=account
    )

    signal = ManualSignal(
        symbol=symbol.upper(),
        direction=direction.upper(),
        action=action.upper(),
        source=source,
        entry_price=entry_price,
        close_percent=close_percent,
        metadata=metadata or {}
    )

    success = store.push_signal(signal)

    if success:
        print(f"\nSignal queued successfully!")
        print(f"  Signal ID: {signal.signal_id}")
        print(f"  Symbol: {signal.symbol}")
        print(f"  Action: {signal.action}")
        print(f"  Direction: {signal.direction}")
        print(f"  Source: {signal.source}")
        if entry_price:
            print(f"  Entry Price: {entry_price}")
        if signal.action == "EXIT" and close_percent:
            print(f"  Close Percent: {close_percent}%")
        print(f"  Timestamp: {signal.timestamp}")
        print(f"\nThe signal will be processed by ManualSignalService")
        if signal.action == "EXIT":
            direction_text = "long" if signal.direction == "BUY" else "short"
            pct_text = f"{close_percent}% of" if close_percent and close_percent < 100 else "all"
            print(f"and will close {pct_text} {direction_text} positions.")
        else:
            print(f"and routed through the risk management pipeline.")
    else:
        print(f"\nFailed to queue signal (may be duplicate)")

    return success


def list_pending_signals(client: redis.Redis, account: str) -> None:
    """List pending signals in the queue."""
    store = ManualSignalStore(
        redis_client=client,
        account_name=account
    )

    signals = store.peek_signals(limit=50)
    queue_length = store.get_queue_length()

    print(f"\n{'='*60}")
    print(f"Pending Signals for account: {account}")
    print(f"{'='*60}")

    if not signals:
        print("\nNo pending signals in queue")
        return

    print(f"\nTotal pending: {queue_length}")
    print()

    for i, signal in enumerate(signals, 1):
        action = getattr(signal, 'action', 'ENTRY')
        print(f"[{i}] {signal.symbol} {action} {signal.direction}")
        print(f"    ID: {signal.signal_id}")
        print(f"    Source: {signal.source}")
        print(f"    Timestamp: {signal.timestamp}")
        if signal.entry_price:
            print(f"    Entry Price: {signal.entry_price}")
        print()


def show_history(client: redis.Redis, account: str, limit: int = 20) -> None:
    """Show signal processing history."""
    store = ManualSignalStore(
        redis_client=client,
        account_name=account
    )

    history = store.get_history(limit=limit)

    print(f"\n{'='*60}")
    print(f"Signal History for account: {account} (last {limit})")
    print(f"{'='*60}")

    if not history:
        print("\nNo signal history")
        return

    for i, entry in enumerate(history, 1):
        result = entry.get('result', {})
        status = result.get('status', 'unknown')
        status_icon = 'v' if status == 'published' else 'x'
        action = entry.get('action', 'ENTRY')

        print(f"\n[{status_icon}] {entry.get('symbol')} {action} {entry.get('direction')}")
        print(f"    ID: {entry.get('signal_id')}")
        print(f"    Source: {entry.get('source')}")
        print(f"    Submitted: {entry.get('timestamp')}")
        print(f"    Processed: {entry.get('processed_at')}")
        print(f"    Status: {status}")
        if status == 'failed':
            print(f"    Error: {result.get('error')}")
        elif status == 'published':
            print(f"    Correlation: {result.get('correlation_id')}")


def clear_queue(client: redis.Redis, account: str) -> None:
    """Clear all pending signals."""
    store = ManualSignalStore(
        redis_client=client,
        account_name=account
    )

    count = store.clear_queue()
    print(f"\nCleared {count} pending signals from queue")


def main():
    parser = argparse.ArgumentParser(
        description='Push manual trading signals to Redis',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Push a BUY entry signal (opens a long position):
    python push_manual_signal.py --symbol XAUUSD --direction BUY

  Push a SELL entry signal (opens a short position):
    python push_manual_signal.py --symbol XAUUSD --direction SELL --source telegram

  Push an EXIT signal to close all long positions:
    python push_manual_signal.py --symbol XAUUSD --direction BUY --action EXIT

  Push an EXIT signal to close 50% of long positions:
    python push_manual_signal.py --symbol XAUUSD --direction BUY --action EXIT --close-percent 50

  Push an EXIT signal to close all short positions:
    python push_manual_signal.py --symbol XAUUSD --direction SELL --action EXIT

  Push with specific price:
    python push_manual_signal.py --symbol XAUUSD --direction BUY --price 2650.50

  List pending signals:
    python push_manual_signal.py --list

  View history:
    python push_manual_signal.py --history
        """
    )

    # Connection args
    parser.add_argument('--host', default='localhost', help='Redis host (default: localhost)')
    parser.add_argument('--port', type=int, default=6379, help='Redis port (default: 6379)')
    parser.add_argument('--db', type=int, default=1, help='Redis database (default: 1)')
    parser.add_argument('--account', default='acg_daily', help='Account name (default: acg_daily)')

    # Signal args
    parser.add_argument('--symbol', '-s', help='Trading symbol (e.g., XAUUSD)')
    parser.add_argument('--direction', '-d', choices=['BUY', 'SELL', 'buy', 'sell'],
                        help='Trade direction')
    parser.add_argument('--action', '-a', choices=['ENTRY', 'EXIT', 'entry', 'exit'],
                        default='ENTRY',
                        help='Signal action: ENTRY (open position) or EXIT (close position)')
    parser.add_argument('--close-percent', type=float, default=None,
                        help='Percentage of position to close (1-100, only for EXIT, default: 100)')
    parser.add_argument('--source', default='manual_cli', help='Signal source (default: manual_cli)')
    parser.add_argument('--price', '-p', type=float, help='Entry price (optional)')

    # Actions
    parser.add_argument('--list', '-l', action='store_true', help='List pending signals')
    parser.add_argument('--history', '-H', action='store_true', help='Show signal history')
    parser.add_argument('--clear', '-c', action='store_true', help='Clear pending signals')

    args = parser.parse_args()

    # Connect to Redis
    client = connect_redis(args.host, args.port, args.db)

    # Handle actions
    if args.list:
        list_pending_signals(client, args.account)
    elif args.history:
        show_history(client, args.account)
    elif args.clear:
        clear_queue(client, args.account)
    elif args.symbol and args.direction:
        # Get close_percent (argparse uses underscore for attribute name)
        close_pct = getattr(args, 'close_percent', None)
        push_signal(
            client=client,
            account=args.account,
            symbol=args.symbol,
            direction=args.direction,
            action=args.action,
            source=args.source,
            entry_price=args.price,
            close_percent=close_pct
        )
    else:
        parser.print_help()
        print("\nError: Must specify --symbol and --direction, or use --list/--history/--clear")
        sys.exit(1)


if __name__ == "__main__":
    main()
