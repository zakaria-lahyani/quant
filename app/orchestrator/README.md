# Trading Orchestrator Architecture

Event-driven orchestrator system for managing multi-symbol trading operations.

## Architecture Overview

```
┌────────────────────────────────────────────────────────────────┐
│           MultiSymbolTradingOrchestrator                        │
│                                                                 │
│  - Manages multiple symbol orchestrators                       │
│  - Coordinates Redis event bus (external events)               │
│  - Global risk management                                      │
│  - System health monitoring                                    │
└────────────────────────────────────────────────────────────────┘
                           │
                           │ creates & manages
                           ▼
        ┌──────────────────────────────────────────┐
        │     Symbol Orchestrators (per symbol)     │
        │                                          │
        │  ┌─────────────┐    ┌──────────────┐    │
        │  │   BTCUSD    │    │    XAUUSD    │    │
        │  │ Orchestrator│    │ Orchestrator │    │
        │  └─────────────┘    └──────────────┘    │
        └──────────────────────────────────────────┘
                           │
                           │ manages
                           ▼
        ┌──────────────────────────────────────────┐
        │      Event-Driven Services               │
        │                                          │
        │  1. DataFetchingService                  │
        │  2. IndicatorCalculationService          │
        │  3. StrategyEvaluationService           │
        │  4. TradeExecutionService               │
        └──────────────────────────────────────────┘
```

## Components

### 1. MultiSymbolTradingOrchestrator

**File**: [multi_symbol_orchestrator.py](multi_symbol_orchestrator.py)

Top-level orchestrator managing all symbols.

**Responsibilities**:
- Create SymbolOrchestrator for each symbol
- Inject Redis event bus for external event publishing
- Global health monitoring
- Unified start/stop/restart control
- Auto-restart failed services (configurable)

**Usage**:
```python
from app.orchestrator import MultiSymbolTradingOrchestrator

orchestrator = MultiSymbolTradingOrchestrator(
    system_config=system_config,
    symbol_components=symbol_components,
    client=client,
    data_source=data_source,
    redis_event_bus=event_bus,
    logger=logger
)

# Start all symbols
orchestrator.start()

# Run with periodic health checks
orchestrator.run(check_interval=60)

# Or manual control
orchestrator.pause()
orchestrator.resume()
orchestrator.stop()
```

### 2. SymbolOrchestrator

**File**: [symbol_orchestrator.py](symbol_orchestrator.py)

Manages services for a single trading symbol.

**Responsibilities**:
- Create local event bus for service coordination
- Initialize event-driven services
- Inject event buses into components (Redis + Local)
- Service lifecycle management
- Per-symbol health monitoring

**Key Features**:
- **Local Event Bus**: For in-process service coordination
- **Redis Event Bus Injection**: Injects into TradeExecutor for external events
- **Service Coordination**: Services communicate via events
- **State Management**: INITIALIZING → RUNNING → PAUSED/STOPPED/ERROR

### 3. Event System

**Files**:
- [events.py](events.py) - Orchestrator event definitions
- [services/event_bus.py](services/event_bus.py) - Local event bus

**Two Event Buses**:

1. **Local Event Bus** (per symbol)
   - In-process communication
   - Service coordination within symbol
   - Fast, synchronous
   - Events: NEW_CANDLE, INDICATORS_UPDATED, SIGNAL_GENERATED, etc.

2. **Redis Event Bus** (global)
   - External communication
   - Cross-system events
   - Distributed, pub/sub
   - Events: ORDER_CREATED, POSITION_OPENED, TRADE_EXECUTED, etc.

**Event Flow**:
```
DataFetchingService
    │
    └─[NEW_CANDLE]─────────▶ Local Event Bus
                                │
                                ▼
                    IndicatorCalculationService
                                │
                                └─[INDICATORS_UPDATED]───▶ Local Event Bus
                                                              │
                                                              ▼
                                                   StrategyEvaluationService
                                                              │
                                                              └─[SIGNAL_GENERATED]───▶ Local Event Bus
                                                                                          │
                                                                                          ▼
                                                                               TradeExecutionService
                                                                                          │
                                                                                          ├─[ORDER_CREATED]─────▶ Redis Event Bus
                                                                                          └─[POSITION_OPENED]───▶ Redis Event Bus
```

### 4. Service Base Classes

**File**: [services/base.py](services/base.py)

Base class for all event-driven services.

**Service Lifecycle**:
```
CREATED → STARTING → RUNNING ⇄ PAUSED
                        │
                        ├─────▶ ERROR → RECOVERING ──▶ RUNNING
                        │
                        └─────▶ STOPPING → STOPPED
```

**Key Features**:
- Automatic error handling
- Recovery mechanism (up to 3 attempts)
- State management
- Health monitoring
- Metrics and status reporting

## Event Types

### Orchestrator Events (Local)

Defined in [events.py](events.py):

- `NEW_CANDLE` - New candle data available
- `DATA_FETCH_FAILED` - Data fetch error
- `INDICATORS_UPDATED` - Indicators calculated
- `REGIME_CHANGED` - Market regime changed
- `SIGNAL_GENERATED` - Trading signal created
- `SIGNAL_VALIDATED` - Signal passed validation
- `SIGNAL_REJECTED` - Signal rejected
- `TRADE_EXECUTED` - Trade executed successfully
- `TRADE_FAILED` - Trade execution failed
- `SERVICE_STARTED/STOPPED/ERROR` - Service lifecycle
- `ORCHESTRATOR_STARTED/STOPPED/PAUSED` - Orchestrator lifecycle

### Trade Events (Redis)

Defined in [app/infrastructure/events/event_types.py](../infrastructure/events/event_types.py):

- `ORDER_CREATED/FILLED/CANCELLED` - Order events
- `POSITION_OPENED/CLOSED/MODIFIED` - Position events
- `RISK_LIMIT_BREACHED/STOP_LOSS_HIT` - Risk events

## Service Implementation (TODO)

Services to be implemented in [services/](services/) directory:

### DataFetchingService
- Fetches new candle data periodically
- Publishes `NEW_CANDLE` events
- Configuration: fetch_interval, retry_attempts, candle_index

### IndicatorCalculationService
- Subscribes to `NEW_CANDLE` events
- Updates indicator_processor with new data
- Calculates indicators and checks regime changes
- Publishes `INDICATORS_UPDATED` and `REGIME_CHANGED` events

### StrategyEvaluationService
- Subscribes to `INDICATORS_UPDATED` events
- Evaluates strategy_engine for signals
- Uses entry_manager for signal validation
- Publishes `SIGNAL_GENERATED` events

### TradeExecutionService
- Subscribes to `SIGNAL_GENERATED` events
- Validates signals (restrictions, risk limits)
- Executes trades via trade_executor
- Publishes `TRADE_EXECUTED` events (to both Local and Redis event buses)

## Configuration

Services are configured via [services.yaml](../../configs/services.yaml):

```yaml
services:
  data_fetching:
    enabled: true
    fetch_interval: 30
    retry_attempts: 3
    candle_index: 2
    nbr_bars: 2

  indicator_calculation:
    enabled: true
    recent_rows_limit: 6
    track_regime_changes: true

  strategy_evaluation:
    enabled: true
    evaluation_mode: "on_new_candle"
    min_rows_required: 3

  trade_execution:
    enabled: true
    execution_mode: "immediate"

orchestrator:
  enable_auto_restart: true
  health_check_interval: 60
  status_log_interval: 10
```

## Usage Example

```python
# In main.py

from app.infrastructure.events import EventBusFactory
from app.orchestrator import MultiSymbolTradingOrchestrator
from app.utils.load_component import load_all_components_for_symbols

# 1. Create Redis event bus (for external events)
event_bus = EventBusFactory.create_from_env(env_config, logger)

# 2. Load components for all symbols
symbol_components = load_all_components_for_symbols(
    config_path=config_path,
    env_config=env_config,
    system_config=system_config,
    client=client,
    data_source=data_source,
    logger=logger
)

# 3. Create multi-symbol orchestrator
orchestrator = MultiSymbolTradingOrchestrator(
    system_config=system_config,
    symbol_components=symbol_components,
    client=client,
    data_source=data_source,
    redis_event_bus=event_bus,  # Inject here
    logger=logger
)

# 4. Run (starts all services and monitors health)
orchestrator.run(check_interval=60)
```

## State Management

### Orchestrator States

- `INITIALIZING` - Loading components, creating services
- `RUNNING` - All services active
- `PAUSED` - Services paused (e.g., market closed, risk breach)
- `STOPPED` - Clean shutdown
- `ERROR` - Exception occurred
- `RECOVERING` - Attempting service restart

### Service States

- `CREATED` - Service instantiated
- `STARTING` - Initializing resources
- `RUNNING` - Active and processing events
- `PAUSED` - Temporarily suspended
- `STOPPING` - Cleaning up resources
- `STOPPED` - Completely shut down
- `ERROR` - Exception occurred
- `RECOVERING` - Attempting recovery

## Health Monitoring

### Get Health Status

```python
# Multi-symbol orchestrator
health = orchestrator.check_health()
print(health)
# {
#     'state': 'running',
#     'is_healthy': True,
#     'healthy_symbols': 2,
#     'total_symbols': 2,
#     'symbols': {
#         'BTCUSD': {...},
#         'XAUUSD': {...}
#     },
#     'uptime': 3600.0,
#     'redis_connected': True
# }

# Symbol orchestrator
health = orchestrator.orchestrators['XAUUSD'].check_health()
# {
#     'symbol': 'XAUUSD',
#     'state': 'running',
#     'is_healthy': True,
#     'healthy_services': 4,
#     'total_services': 4,
#     'services': [...],
#     'uptime': 3600.0
# }

# Service
service = orchestrator.orchestrators['XAUUSD'].services[0]
status = service.get_status()
# {
#     'service_name': 'DataFetchingService',
#     'symbol': 'XAUUSD',
#     'state': 'running',
#     'is_healthy': True,
#     'error_count': 0,
#     'uptime': 3600.0
# }
```

### Auto-Restart

If `enable_auto_restart: true` in config, orchestrator will:
1. Periodically check service health
2. Automatically restart failed services
3. Log recovery attempts
4. Escalate to symbol restart if service restart fails

## Error Handling

Services handle errors gracefully:

1. **Error Occurs**: Service catches exception
2. **State Transition**: Service → ERROR state
3. **Recovery Attempt**: Service tries to restart (up to 3 attempts)
4. **Escalation**: If recovery fails, orchestrator restarts entire symbol
5. **Logging**: All errors logged with context

## Next Steps

1. **Implement Services**: Create DataFetchingService, IndicatorCalculationService, etc.
2. **Wire Up Components**: Connect existing components to services
3. **Testing**: Unit tests for each service
4. **Integration**: Test full event flow
5. **Monitoring**: Add metrics and dashboards
6. **Deployment**: Docker containerization

## Directory Structure

```
app/orchestrator/
├── __init__.py                          # Package exports
├── README.md                            # This file
├── events.py                            # Orchestrator event definitions
├── symbol_orchestrator.py               # Per-symbol orchestrator
├── multi_symbol_orchestrator.py         # Multi-symbol orchestrator
└── services/
    ├── __init__.py
    ├── base.py                          # Base service class
    ├── event_bus.py                     # Local event bus
    ├── data_fetching.py                 # TODO
    ├── indicator_calculation.py         # TODO
    ├── strategy_evaluation.py           # TODO
    └── trade_execution.py               # TODO
```
