"""
Multi-symbol orchestrator for managing trading across multiple symbols.
"""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import threading
from apscheduler.schedulers.background import BackgroundScheduler

from app.infrastructure.configs.config_definitions import SystemConfig
from app.infrastructure.events import RedisEventBus
from app.orchestrator.symbol_orchestrator import SymbolOrchestrator, OrchestratorState
from app.orchestrator.services.data_fetching import DataFetchingService


class MultiSymbolTradingOrchestrator:
    """
    Top-level orchestrator for managing trading across multiple symbols.

    Responsibilities:
    - Create and manage symbol orchestrators
    - Coordinate Redis event bus for cross-symbol communication
    - Handle global risk management (account-level stop loss)
    - Monitor system health across all symbols
    - Provide unified start/stop/restart control
    """

    def __init__(
        self,
        system_config: SystemConfig,
        symbol_components: Dict[str, Dict[str, Any]],
        client: Any,
        data_source: Any,
        redis_event_bus: Optional[RedisEventBus] = None,
        account_name: Optional[str] = None,
        account_tag: Optional[str] = None,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize multi-symbol orchestrator.

        Args:
            system_config: System configuration from services.yaml
            symbol_components: Components for each symbol
            client: MT5 client (shared)
            data_source: Data source manager (shared)
            redis_event_bus: Optional Redis event bus for external events
            account_name: Account/broker name for Redis metadata (e.g., "ACG", "FTMO")
            account_tag: Account tag identifier
            logger: Optional logger instance
        """
        self.system_config = system_config
        self.symbol_components = symbol_components
        self.client = client
        self.data_source = data_source
        self.redis_event_bus = redis_event_bus
        self.account_name = account_name
        self.account_tag = account_tag
        self.logger = logger or logging.getLogger('multi-symbol-orchestrator')

        # Symbol orchestrators
        self.orchestrators: Dict[str, SymbolOrchestrator] = {}

        # State
        self._state = OrchestratorState.INITIALIZING
        self._start_time: Optional[datetime] = None

        # Scheduler for periodic data fetching
        self.scheduler: Optional[BackgroundScheduler] = None

        # Initialize orchestrators for each symbol
        self._initialize_orchestrators()

    def _initialize_orchestrators(self) -> None:
        """Initialize orchestrator for each symbol."""
        self.logger.info("Initializing symbol orchestrators...")

        for symbol, components in self.symbol_components.items():
            self.logger.info(f"  Creating orchestrator for {symbol}...")

            orchestrator = SymbolOrchestrator(
                symbol=symbol,
                symbol_components=components,
                system_config=self.system_config,
                client=self.client,
                data_source=self.data_source,
                redis_event_bus=self.redis_event_bus,
                account_name=self.account_name,
                account_tag=self.account_tag,
                logger=logging.getLogger(f'orchestrator-{symbol.lower()}')
            )

            self.orchestrators[symbol] = orchestrator
            self.logger.info(f"  ✓ Orchestrator created for {symbol}")

        self.logger.info(
            f"Symbol orchestrators initialized for {len(self.orchestrators)} symbols"
        )

    def start(self) -> None:
        """Start all symbol orchestrators."""
        self.logger.info("=" * 60)
        self.logger.info("Starting Multi-Symbol Trading Orchestrator")
        self.logger.info("=" * 60)

        self._state = OrchestratorState.RUNNING
        self._start_time = datetime.utcnow()

        # Start each symbol orchestrator
        for symbol, orchestrator in self.orchestrators.items():
            try:
                self.logger.info(f"\nStarting orchestrator for {symbol}...")
                orchestrator.start()
                self.logger.info(f"✓ {symbol} orchestrator started")
            except Exception as e:
                self.logger.error(f"✗ Failed to start {symbol} orchestrator: {e}")

        # Start scheduler for periodic data fetching
        if self.system_config.services.data_fetching.enabled:
            self.logger.info("\nStarting data fetch scheduler...")
            self.scheduler = BackgroundScheduler()

            for symbol, orchestrator in self.orchestrators.items():
                for service in orchestrator.services:
                    if isinstance(service, DataFetchingService):
                        fetch_interval = self.system_config.services.data_fetching.fetch_interval
                        self.scheduler.add_job(
                            service.fetch_and_publish,
                            'interval',
                            seconds=fetch_interval,
                            id=f'fetch_{symbol}',
                            name=f'Data Fetch - {symbol}'
                        )
                        self.logger.info(f"  ✓ Scheduled data fetch for {symbol} (every {fetch_interval}s)")

            self.scheduler.start()
            self.logger.info("Data fetch scheduler started")

        self.logger.info("\n" + "=" * 60)
        self.logger.info("Multi-Symbol Trading Orchestrator Started")
        self.logger.info(f"Active Symbols: {list(self.orchestrators.keys())}")
        self.logger.info("=" * 60 + "\n")

    def stop(self) -> None:
        """Stop all symbol orchestrators."""
        self.logger.info("=" * 60)
        self.logger.info("Stopping Multi-Symbol Trading Orchestrator")
        self.logger.info("=" * 60)

        self._state = OrchestratorState.STOPPING

        # Stop scheduler first
        if self.scheduler:
            self.logger.info("\nStopping data fetch scheduler...")
            try:
                self.scheduler.shutdown(wait=True)
                self.logger.info("✓ Data fetch scheduler stopped")
            except Exception as e:
                self.logger.error(f"✗ Failed to stop scheduler: {e}")

        # Stop each symbol orchestrator
        for symbol, orchestrator in self.orchestrators.items():
            try:
                self.logger.info(f"\nStopping orchestrator for {symbol}...")
                orchestrator.stop()
                self.logger.info(f"✓ {symbol} orchestrator stopped")
            except Exception as e:
                self.logger.error(f"✗ Failed to stop {symbol} orchestrator: {e}")

        self._state = OrchestratorState.STOPPED

        self.logger.info("\n" + "=" * 60)
        self.logger.info("Multi-Symbol Trading Orchestrator Stopped")
        self.logger.info("=" * 60 + "\n")

    def pause(self) -> None:
        """Pause all symbol orchestrators."""
        self.logger.info("Pausing all symbol orchestrators...")
        self._state = OrchestratorState.PAUSED

        for symbol, orchestrator in self.orchestrators.items():
            orchestrator.pause()
            self.logger.info(f"  ✓ {symbol} orchestrator paused")

    def resume(self) -> None:
        """Resume all symbol orchestrators."""
        self.logger.info("Resuming all symbol orchestrators...")
        self._state = OrchestratorState.RUNNING

        for symbol, orchestrator in self.orchestrators.items():
            orchestrator.resume()
            self.logger.info(f"  ✓ {symbol} orchestrator resumed")

    def restart_symbol(self, symbol: str) -> bool:
        """
        Restart orchestrator for a specific symbol.

        Args:
            symbol: Trading symbol to restart

        Returns:
            True if restart successful
        """
        if symbol not in self.orchestrators:
            self.logger.warning(f"Symbol not found: {symbol}")
            return False

        try:
            self.logger.info(f"Restarting orchestrator for {symbol}...")
            orchestrator = self.orchestrators[symbol]
            orchestrator.stop()
            orchestrator.start()
            self.logger.info(f"✓ {symbol} orchestrator restarted")
            return True
        except Exception as e:
            self.logger.error(f"✗ Failed to restart {symbol} orchestrator: {e}")
            return False

    def restart_service(self, symbol: str, service_name: str) -> bool:
        """
        Restart a specific service for a symbol.

        Args:
            symbol: Trading symbol
            service_name: Name of service to restart

        Returns:
            True if restart successful
        """
        if symbol not in self.orchestrators:
            self.logger.warning(f"Symbol not found: {symbol}")
            return False

        return self.orchestrators[symbol].restart_service(service_name)

    def check_health(self) -> Dict[str, Any]:
        """
        Check health of all orchestrators and services.

        Returns:
            Health status dictionary
        """
        symbol_statuses = {}
        healthy_count = 0

        for symbol, orchestrator in self.orchestrators.items():
            status = orchestrator.check_health()
            symbol_statuses[symbol] = status
            if status['is_healthy']:
                healthy_count += 1

        is_healthy = (
            self._state == OrchestratorState.RUNNING and
            healthy_count == len(self.orchestrators)
        )

        return {
            'state': self._state.value,
            'is_healthy': is_healthy,
            'healthy_symbols': healthy_count,
            'total_symbols': len(self.orchestrators),
            'symbols': symbol_statuses,
            'uptime': (
                (datetime.utcnow() - self._start_time).total_seconds()
                if self._start_time else 0
            ),
            'redis_connected': (
                self.redis_event_bus.is_connected
                if self.redis_event_bus else False
            )
        }

    def get_status(self) -> Dict[str, Any]:
        """Get orchestrator status (alias for check_health)."""
        return self.check_health()

    def log_status(self) -> None:
        """Log current status of all orchestrators."""
        health = self.check_health()

        self.logger.info("\n" + "=" * 60)
        self.logger.info("Multi-Symbol Orchestrator Status")
        self.logger.info("=" * 60)
        self.logger.info(f"State: {health['state']}")
        self.logger.info(f"Healthy: {health['is_healthy']}")
        self.logger.info(
            f"Symbols: {health['healthy_symbols']}/{health['total_symbols']} healthy"
        )
        self.logger.info(f"Uptime: {health['uptime']:.1f}s")
        self.logger.info(f"Redis Connected: {health['redis_connected']}")

        self.logger.info("\nSymbol Status:")
        for symbol, status in health['symbols'].items():
            self.logger.info(
                f"  {symbol}: {status['state']} "
                f"({status['healthy_services']}/{status['total_services']} services healthy)"
            )

        self.logger.info("=" * 60 + "\n")

    def run(self, check_interval: int = 60) -> None:
        """
        Run orchestrator with periodic health checks.

        Args:
            check_interval: Seconds between health checks
        """
        self.start()

        try:
            while self._state == OrchestratorState.RUNNING:
                import time
                time.sleep(check_interval)

                # Periodic health check
                self.log_status()

                # Auto-restart unhealthy services if configured
                if self.system_config.orchestrator.enable_auto_restart:
                    self._auto_restart_unhealthy()

        except KeyboardInterrupt:
            self.logger.info("\nShutdown requested by user")
        except Exception as e:
            self.logger.error(f"Orchestrator error: {e}", exc_info=True)
        finally:
            self.stop()

    def _auto_restart_unhealthy(self) -> None:
        """Automatically restart unhealthy services."""
        for symbol, orchestrator in self.orchestrators.items():
            if not orchestrator.is_healthy:
                self.logger.warning(f"{symbol} orchestrator is unhealthy, attempting restart...")
                self.restart_symbol(symbol)

    @property
    def state(self) -> OrchestratorState:
        """Get current orchestrator state."""
        return self._state

    @property
    def is_running(self) -> bool:
        """Check if orchestrator is running."""
        return self._state == OrchestratorState.RUNNING

    @property
    def is_healthy(self) -> bool:
        """Check if orchestrator is healthy."""
        health = self.check_health()
        return health['is_healthy']

    def __repr__(self) -> str:
        return (
            f"<MultiSymbolTradingOrchestrator "
            f"symbols={list(self.orchestrators.keys())} "
            f"state={self._state.value}>"
        )
