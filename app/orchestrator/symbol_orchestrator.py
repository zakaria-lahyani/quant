"""
Symbol-specific orchestrator for managing trading services for a single symbol.
"""

import logging
from typing import Dict, List, Any, Optional
from enum import Enum
from datetime import datetime

from app.infrastructure.configs.config_definitions import SystemConfig
from app.orchestrator.services.base import TradingService, ServiceState
from app.orchestrator.services.event_bus import LocalEventBus
from app.orchestrator.events import OrchestratorEventType
from app.utils.date_helper import DateHelper


class OrchestratorState(str, Enum):
    """Orchestrator lifecycle states."""
    INITIALIZING = "initializing"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"
    RECOVERING = "recovering"


class SymbolOrchestrator:
    """
    Orchestrator for managing all services for a single trading symbol.

    Responsibilities:
    - Create and manage local event bus
    - Initialize and coordinate services
    - Handle service lifecycle (start/stop/restart)
    - Monitor service health
    - Inject event bus into components
    """

    def __init__(
        self,
        symbol: str,
        symbol_components: Dict[str, Any],
        system_config: SystemConfig,
        client: Any,
        data_source: Any,
        redis_event_bus: Optional[Any] = None,
        account_name: Optional[str] = None,
        account_tag: Optional[str] = None,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize symbol orchestrator.

        Args:
            symbol: Trading symbol (e.g., "XAUUSD")
            symbol_components: Components loaded for this symbol
            system_config: System configuration
            client: MT5 client
            data_source: Data source manager
            redis_event_bus: Optional Redis event bus for external events
            account_name: Account/broker name for Redis metadata (e.g., "ACG", "FTMO")
            account_tag: Account tag identifier
            logger: Optional logger instance
        """
        self.symbol = symbol
        self.broker_symbol = symbol_components.get('broker_symbol', symbol)  # Broker-specific symbol for API calls
        self.symbol_components = symbol_components
        self.system_config = system_config
        self.client = client
        self.data_source = data_source
        self.redis_event_bus = redis_event_bus
        self.account_name = account_name
        self.account_tag = account_tag
        self.logger = logger or logging.getLogger(f'orchestrator-{symbol.lower()}')

        # Create date helper for time calculations
        self.date_helper = DateHelper()

        # State
        self._state = OrchestratorState.INITIALIZING
        self._start_time: Optional[datetime] = None

        # Local event bus for service coordination
        self.event_bus = LocalEventBus(symbol, self.logger)

        # Services
        self.services: List[TradingService] = []

        # Initialize services
        self._initialize_services()

    def _initialize_services(self) -> None:
        """Initialize all services for this symbol."""
        self.logger.info(f"Initializing services for {self.symbol}...")

        # Import services
        from app.orchestrator.services.data_fetching import DataFetchingService
        from app.orchestrator.services.indicator_calculation import IndicatorCalculationService
        from app.orchestrator.services.strategy_evaluation import StrategyEvaluationService
        from app.orchestrator.services.trade_execution import TradeExecutionService

        # Get symbol-specific configuration
        timeframes = self.symbol_components.get('timeframes', [])
        historicals = self.symbol_components.get('historicals', {})

        # 1. Data Fetching Service
        if self.system_config.services.data_fetching.enabled:
            data_fetching_config = self.system_config.get_data_fetching_config(self.symbol)
            data_fetching_service = DataFetchingService(
                symbol=self.broker_symbol,  # Broker-specific symbol for MT5 API calls
                timeframes=timeframes,
                data_source=self.data_source,
                event_bus=self.event_bus,
                config=data_fetching_config,
                display_symbol=self.symbol,  # Display symbol for event routing
                logger=logging.getLogger(f'data-fetching-{self.symbol.lower()}')
            )
            self.services.append(data_fetching_service)
            self.logger.info(f"  ✓ DataFetchingService created")

        # 2. Indicator Calculation Service
        if self.system_config.services.indicator_calculation.enabled:
            indicator_config = self.system_config.get_indicator_calculation_config(self.symbol)

            # Get Redis client if available
            redis_client = None
            if self.redis_event_bus and hasattr(self.redis_event_bus, '_client'):
                redis_client = self.redis_event_bus._client

            indicator_service = IndicatorCalculationService(
                symbol=self.symbol,
                indicator_processor=self.symbol_components['indicator_processor'],
                regime_manager=self.symbol_components['regime_manager'],
                historicals=historicals,
                event_bus=self.event_bus,
                config=indicator_config,
                redis_client=redis_client,
                account_name=self.account_name,
                account_tag=self.account_tag,
                logger=logging.getLogger(f'indicator-calc-{self.symbol.lower()}')
            )
            self.services.append(indicator_service)
            self.logger.info(f"  ✓ IndicatorCalculationService created")

        # 3. Strategy Evaluation Service
        if self.system_config.services.strategy_evaluation.enabled:
            strategy_config = self.system_config.get_strategy_evaluation_config(self.symbol)
            strategy_service = StrategyEvaluationService(
                symbol=self.symbol,
                strategy_engine=self.symbol_components['strategy_engine'],
                entry_manager=self.symbol_components['entry_manager'],
                indicator_processor=self.symbol_components['indicator_processor'],
                historicals=historicals,
                event_bus=self.event_bus,
                config=strategy_config,
                logger=logging.getLogger(f'strategy-eval-{self.symbol.lower()}')
            )
            self.services.append(strategy_service)
            self.logger.info(f"  ✓ StrategyEvaluationService created")

        # 4. Trade Execution Service
        if self.system_config.services.trade_execution.enabled:
            execution_config = self.system_config.get_trade_execution_config(self.symbol)
            execution_service = TradeExecutionService(
                symbol=self.symbol,
                trade_executor=self.symbol_components['trade_executor'],
                entry_manager=self.symbol_components['entry_manager'],
                client=self.client,
                date_helper=self.date_helper,
                redis_event_bus=self.redis_event_bus,
                event_bus=self.event_bus,
                config=execution_config,
                logger=logging.getLogger(f'trade-exec-{self.symbol.lower()}')
            )
            self.services.append(execution_service)
            self.logger.info(f"  ✓ TradeExecutionService created")

        self.logger.info(f"Services initialized for {self.symbol}: {len(self.services)} services created")

    def inject_event_bus_into_executor(self) -> None:
        """
        Inject Redis event bus into TradeExecutor.

        This allows the executor to publish trade events to external systems.
        """
        if self.redis_event_bus:
            trade_executor = self.symbol_components.get('trade_executor')
            if trade_executor:
                # The executor's OrderExecutor component accepts event_bus
                if hasattr(trade_executor, 'order_executor'):
                    trade_executor.order_executor.event_bus = self.redis_event_bus
                    self.logger.info(
                        f"Injected Redis event bus into TradeExecutor for {self.symbol}"
                    )

    def start(self) -> None:
        """Start all services."""
        self.logger.info(f"Starting orchestrator for {self.symbol}...")
        self._state = OrchestratorState.RUNNING
        self._start_time = datetime.utcnow()

        # Inject event bus into executor
        self.inject_event_bus_into_executor()

        # Start all services
        for service in self.services:
            try:
                service.start()
                self.logger.info(f"  ✓ Started {service.service_name}")
            except Exception as e:
                self.logger.error(f"  ✗ Failed to start {service.service_name}: {e}")
                service.handle_error(e)

        self.logger.info(f"Orchestrator for {self.symbol} started")

        # Publish orchestrator started event
        self.event_bus.publish(
            self.event_bus.create_event(
                event_type=OrchestratorEventType.ORCHESTRATOR_STARTED,
                data={'symbol': self.symbol},
                source_service='SymbolOrchestrator'
            )
        )

    def stop(self) -> None:
        """Stop all services."""
        self.logger.info(f"Stopping orchestrator for {self.symbol}...")
        self._state = OrchestratorState.STOPPING

        # Stop all services in reverse order
        for service in reversed(self.services):
            try:
                service.stop()
                self.logger.info(f"  ✓ Stopped {service.service_name}")
            except Exception as e:
                self.logger.error(f"  ✗ Failed to stop {service.service_name}: {e}")

        self._state = OrchestratorState.STOPPED
        self.logger.info(f"Orchestrator for {self.symbol} stopped")

        # Publish orchestrator stopped event
        self.event_bus.publish(
            self.event_bus.create_event(
                event_type=OrchestratorEventType.ORCHESTRATOR_STOPPED,
                data={'symbol': self.symbol},
                source_service='SymbolOrchestrator'
            )
        )

    def pause(self) -> None:
        """Pause all services."""
        self.logger.info(f"Pausing orchestrator for {self.symbol}...")
        self._state = OrchestratorState.PAUSED

        for service in self.services:
            service.pause()

        self.event_bus.publish(
            self.event_bus.create_event(
                event_type=OrchestratorEventType.ORCHESTRATOR_PAUSED,
                data={'symbol': self.symbol},
                source_service='SymbolOrchestrator'
            )
        )

    def resume(self) -> None:
        """Resume all services."""
        self.logger.info(f"Resuming orchestrator for {self.symbol}...")
        self._state = OrchestratorState.RUNNING

        for service in self.services:
            service.resume()

        self.event_bus.publish(
            self.event_bus.create_event(
                event_type=OrchestratorEventType.ORCHESTRATOR_RESUMED,
                data={'symbol': self.symbol},
                source_service='SymbolOrchestrator'
            )
        )

    def restart_service(self, service_name: str) -> bool:
        """
        Restart a specific service.

        Args:
            service_name: Name of service to restart

        Returns:
            True if restart successful
        """
        for service in self.services:
            if service.service_name == service_name:
                try:
                    self.logger.info(f"Restarting {service_name}...")
                    service.stop()
                    service.start()
                    self.logger.info(f"✓ {service_name} restarted")
                    return True
                except Exception as e:
                    self.logger.error(f"✗ Failed to restart {service_name}: {e}")
                    service.handle_error(e)
                    return False

        self.logger.warning(f"Service not found: {service_name}")
        return False

    def check_health(self) -> Dict[str, Any]:
        """
        Check health of all services.

        Returns:
            Health status dictionary
        """
        service_statuses = []
        healthy_count = 0

        for service in self.services:
            status = service.get_status()
            service_statuses.append(status)
            if status['is_healthy']:
                healthy_count += 1

        is_healthy = (
            self._state == OrchestratorState.RUNNING and
            healthy_count == len(self.services)
        )

        return {
            'symbol': self.symbol,
            'state': self._state.value,
            'is_healthy': is_healthy,
            'healthy_services': healthy_count,
            'total_services': len(self.services),
            'services': service_statuses,
            'uptime': (
                (datetime.utcnow() - self._start_time).total_seconds()
                if self._start_time else 0
            ),
            'event_bus_stats': self.event_bus.get_stats()
        }

    def get_status(self) -> Dict[str, Any]:
        """Get orchestrator status (alias for check_health)."""
        return self.check_health()

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
        return f"<SymbolOrchestrator symbol={self.symbol} state={self._state.value} services={len(self.services)}>"
