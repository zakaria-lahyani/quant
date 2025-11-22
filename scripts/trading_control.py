#!/usr/bin/env python3
"""
Trading Control CLI - Runtime control of trading system.

Controls trading at runtime without restarting the system:
- Master trading switch (stops ALL trading including manual)
- Auto-trading switch (stops automatic strategies, manual still works)
- Per-strategy toggles

Usage:
    # Check status (auto-discovers strategies from config)
    python trading_control.py --account funded_next --status

    # Master kill switch (stops everything)
    python trading_control.py --account funded_next --trading off
    python trading_control.py --account funded_next --trading on

    # Disable auto strategies only (manual still works)
    python trading_control.py --account funded_next --auto-trading off
    python trading_control.py --account funded_next --auto-trading on

    # Disable specific strategy
    python trading_control.py --account funded_next --strategy manual off
    python trading_control.py --account funded_next --strategy anchors-transitions on

    # Reset all controls to defaults (all enabled)
    python trading_control.py --account funded_next --reset
"""

import argparse
import sys
from pathlib import Path
from typing import Optional, List, Set

import redis
import yaml

# Add parent directory to path for imports
ROOT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT_DIR))

from app.infrastructure.events.runtime_control_store import RuntimeControlStore


def discover_strategies(account: str) -> Set[str]:
    """
    Discover all strategies for an account by reading strategy config files.

    Args:
        account: Account name (folder name in configs/)

    Returns:
        Set of strategy names found across all symbols
    """
    strategies = set()
    config_path = ROOT_DIR / "configs" / account / "strategies"

    if not config_path.exists():
        return strategies

    # Iterate through all symbol folders
    for symbol_folder in config_path.iterdir():
        if not symbol_folder.is_dir():
            continue

        # Read each strategy YAML file
        for strategy_file in symbol_folder.glob("*.yaml"):
            try:
                with open(strategy_file, 'r') as f:
                    config = yaml.safe_load(f)
                    if config and 'name' in config:
                        strategies.add(config['name'])
            except Exception:
                # Skip files that can't be parsed
                continue

    return strategies


def get_redis_client(host: str, port: int, db: int) -> redis.Redis:
    """Create Redis client."""
    try:
        client = redis.Redis(host=host, port=port, db=db, decode_responses=False)
        client.ping()
        return client
    except redis.ConnectionError as e:
        print(f"Error: Cannot connect to Redis at {host}:{port}")
        print(f"Details: {e}")
        sys.exit(1)


def show_status(store: RuntimeControlStore, account: str) -> None:
    """Display current control status with auto-discovered strategies."""
    status = store.get_status()

    # Discover strategies from config files
    discovered_strategies = discover_strategies(account)

    print("\n" + "=" * 60)
    print(f"Trading Controls - {status['account']}")
    print("=" * 60)

    # Master switch
    trading = status['trading_enabled']
    trading_icon = "[ON]" if trading else "[OFF]"
    trading_color = "" if trading else " (ALL TRADING STOPPED)"
    print(f"\nTrading:      {trading_icon}{trading_color}")

    # Auto-trading switch
    auto = status['auto_trading_enabled']
    auto_icon = "[ON]" if auto else "[OFF]"
    auto_note = "" if auto else " (manual signals only)"
    print(f"Auto-Trading: {auto_icon}{auto_note}")

    # Strategies - merge discovered with Redis state
    redis_strategies = status['strategies']

    # Combine discovered strategies with any in Redis
    all_strategies = discovered_strategies | set(redis_strategies.keys())

    if all_strategies:
        print(f"\nStrategies:")
        for name in sorted(all_strategies):
            # Get state from Redis, default to True (enabled) if not set
            enabled = store.is_strategy_enabled(name)
            icon = "[ON]" if enabled else "[OFF]"
            print(f"  {name}: {icon}")
    else:
        print("\nStrategies: (none found)")

    # Last updated
    if status['last_updated']:
        print(f"\nLast Updated: {status['last_updated']}")

    print("=" * 60 + "\n")


def set_trading(store: RuntimeControlStore, enabled: bool) -> None:
    """Set master trading switch."""
    store.set_trading_enabled(enabled)
    status = "ENABLED" if enabled else "DISABLED"
    print(f"\nTrading {status}")
    if not enabled:
        print("WARNING: ALL trading is now stopped (including manual signals)")
    print()


def set_auto_trading(store: RuntimeControlStore, enabled: bool) -> None:
    """Set auto-trading switch."""
    store.set_auto_trading_enabled(enabled)
    status = "ENABLED" if enabled else "DISABLED"
    print(f"\nAuto-Trading {status}")
    if not enabled:
        print("Note: Automatic strategies disabled, manual signals still work")
    print()


def set_strategy(store: RuntimeControlStore, strategy_name: str, enabled: bool) -> None:
    """Set strategy-specific switch."""
    store.set_strategy_enabled(strategy_name, enabled)
    status = "ENABLED" if enabled else "DISABLED"
    print(f"\nStrategy '{strategy_name}' {status}")
    print()


def reset_controls(store: RuntimeControlStore, account: str) -> None:
    """Reset all controls to defaults."""
    # Get discovered strategies to reset them too
    discovered = discover_strategies(account)
    store.reset_all()

    # Initialize discovered strategies to enabled
    for strategy in discovered:
        store.set_strategy_enabled(strategy, True)

    print("\nAll controls reset to defaults (all enabled)")
    print()


def parse_on_off(value: str) -> bool:
    """Parse on/off string to boolean."""
    if value.lower() in ('on', 'true', '1', 'yes', 'enable', 'enabled'):
        return True
    elif value.lower() in ('off', 'false', '0', 'no', 'disable', 'disabled'):
        return False
    else:
        raise ValueError(f"Invalid value: {value}. Use 'on' or 'off'")


def main():
    parser = argparse.ArgumentParser(
        description='Runtime trading control CLI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Show current status (auto-discovers strategies):
    python trading_control.py --account funded_next --status

  Disable all trading (emergency stop):
    python trading_control.py --account funded_next --trading off

  Enable trading:
    python trading_control.py --account funded_next --trading on

  Disable auto strategies (manual still works):
    python trading_control.py --account funded_next --auto-trading off

  Enable auto strategies:
    python trading_control.py --account funded_next --auto-trading on

  Disable a specific strategy:
    python trading_control.py --account funded_next --strategy anchors-transitions off

  Enable a specific strategy:
    python trading_control.py --account funded_next --strategy manual on

  Reset all controls to defaults:
    python trading_control.py --account funded_next --reset
"""
    )

    # Redis connection
    parser.add_argument('--host', default='localhost', help='Redis host (default: localhost)')
    parser.add_argument('--port', type=int, default=6379, help='Redis port (default: 6379)')
    parser.add_argument('--db', type=int, default=1, help='Redis database (default: 1)')
    parser.add_argument('--account', '-a', default='acg_daily', help='Account name (default: acg_daily)')

    # Actions
    parser.add_argument('--status', '-s', action='store_true', help='Show current control status')
    parser.add_argument('--trading', '-t', metavar='on/off', help='Enable/disable ALL trading (master switch)')
    parser.add_argument('--auto-trading', metavar='on/off', help='Enable/disable auto-trading (manual still works)')
    parser.add_argument('--strategy', nargs=2, metavar=('NAME', 'on/off'), help='Enable/disable specific strategy')
    parser.add_argument('--reset', action='store_true', help='Reset all controls to defaults')

    args = parser.parse_args()

    # Connect to Redis
    client = get_redis_client(args.host, args.port, args.db)
    store = RuntimeControlStore(client, args.account)

    # Handle actions
    action_taken = False

    if args.trading:
        try:
            enabled = parse_on_off(args.trading)
            set_trading(store, enabled)
            action_taken = True
        except ValueError as e:
            print(f"Error: {e}")
            sys.exit(1)

    if args.auto_trading:
        try:
            enabled = parse_on_off(args.auto_trading)
            set_auto_trading(store, enabled)
            action_taken = True
        except ValueError as e:
            print(f"Error: {e}")
            sys.exit(1)

    if args.strategy:
        strategy_name, state = args.strategy
        try:
            enabled = parse_on_off(state)
            set_strategy(store, strategy_name, enabled)
            action_taken = True
        except ValueError as e:
            print(f"Error: {e}")
            sys.exit(1)

    if args.reset:
        reset_controls(store, args.account)
        action_taken = True

    # Show status if requested or no action taken
    if args.status or not action_taken:
        show_status(store, args.account)


if __name__ == "__main__":
    main()
