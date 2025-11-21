# Redis Event Bus

Event-driven infrastructure for distributed trade event management using Redis pub/sub.

## Architecture

The trading application connects to an **external Redis Docker container** to publish and subscribe to trade events. The Redis container manages all operational configuration (streams, TTLs, consumer groups, etc.).

```
┌─────────────────────┐         ┌──────────────────┐         ┌─────────────────────┐
│  Trading App        │         │  Redis Docker    │         │  Other Services     │
│                     │         │  Container       │         │  (Monitoring, etc)  │
│  ┌──────────────┐   │         │                  │         │                     │
│  │ RedisEventBus├───┼────────>│  Pub/Sub         │<────────┼─────────────────────┤
│  └──────────────┘   │         │  Channels        │         │                     │
│                     │         │                  │         └─────────────────────┘
│  - Publish events   │         │  Data Structures:│
│  - Subscribe        │         │  - Hashes        │
│  - Query state      │         │  - Sorted Sets   │
└─────────────────────┘         │  - Streams       │
                                └──────────────────┘
```

## Configuration

### Environment Variables (.env)

```env
# Redis Connection (external Docker container)
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
REDIS_PASSWORD=
```

### Usage in Code

```python
from app.infrastructure.config_loader import LoadEnvironmentVariables
from app.infrastructure.events import EventBusFactory, TradeEvent, TradeEventType

# Load environment configuration
env_config = LoadEnvironmentVariables(".env")

# Create event bus
event_bus = EventBusFactory.create_from_env(env_config)

# Publish an event
event = TradeEvent.create_order_created(
    symbol="XAUUSD",
    order_id="12345",
    order_type="BUY",
    price=2000.50,
    volume=0.1
)
event_bus.publish(event)
```

## Event Types

### Order Events
- `ORDER_CREATED` - New order placed
- `ORDER_FILLED` - Order completely filled
- `ORDER_PARTIALLY_FILLED` - Order partially filled
- `ORDER_CANCELLED` - Order cancelled
- `ORDER_REJECTED` - Order rejected by broker
- `ORDER_MODIFIED` - Order parameters modified

### Position Events
- `POSITION_OPENED` - New position opened
- `POSITION_CLOSED` - Position closed
- `POSITION_MODIFIED` - Position parameters modified
- `POSITION_PARTIALLY_CLOSED` - Position partially closed

### Trade Execution Events
- `TRADE_EXECUTED` - Trade successfully executed
- `TRADE_FAILED` - Trade execution failed

### Risk Events
- `RISK_LIMIT_BREACHED` - Risk limit exceeded
- `STOP_LOSS_HIT` - Stop loss triggered
- `TAKE_PROFIT_HIT` - Take profit triggered

### Strategy Events
- `SIGNAL_GENERATED` - Trading signal generated
- `SIGNAL_VALIDATED` - Signal passed validation
- `SIGNAL_REJECTED` - Signal rejected

## Publishing Events

### Using Helper Methods

```python
# Order created
event = TradeEvent.create_order_created(
    symbol="XAUUSD",
    order_id="12345",
    order_type="BUY",
    price=2000.50,
    volume=0.1,
    sl=1995.00,  # Additional fields
    tp=2010.00
)
event_bus.publish(event)

# Position opened
event = TradeEvent.create_position_opened(
    symbol="XAUUSD",
    position_id="67890",
    entry_price=2000.50,
    volume=0.1,
    direction="LONG"
)
event_bus.publish(event)

# Position closed
event = TradeEvent.create_position_closed(
    symbol="XAUUSD",
    position_id="67890",
    exit_price=2010.00,
    pnl=95.00
)
event_bus.publish(event)
```

### Custom Events

```python
from datetime import datetime

event = TradeEvent(
    event_type=TradeEventType.TRADE_EXECUTED,
    symbol="BTCUSD",
    timestamp=datetime.utcnow().isoformat(),
    data={
        'trade_id': 'T123',
        'strategy': 'momentum',
        'entry': 45000.00,
        'volume': 0.5
    },
    correlation_id="corr-123",
    source="strategy-evaluator"
)
event_bus.publish(event)
```

## Subscribing to Events

### Subscribe to Specific Event Type

```python
def handle_order_created(event: TradeEvent):
    print(f"New order: {event.data['order_id']} at {event.data['price']}")

# Subscribe
event_bus.subscribe("XAUUSD", TradeEventType.ORDER_CREATED, handle_order_created)

# Start listening (blocking)
event_bus.listen()
```

### Subscribe to All Events

```python
def handle_all_events(event: TradeEvent):
    print(f"Event: {event.event_type.value} for {event.symbol}")
    print(f"Data: {event.data}")

# Subscribe to all events for a symbol
event_bus.subscribe_all("XAUUSD", handle_all_events)

# Start listening
event_bus.listen()
```

### Non-blocking Subscription (Threading)

```python
import threading

def event_listener():
    event_bus.subscribe_all("XAUUSD", handle_event)
    event_bus.listen()

# Start listener in background thread
listener_thread = threading.Thread(target=event_listener, daemon=True)
listener_thread.start()

# Main thread continues...
```

## Querying State

The Redis container maintains current state in hashes. You can query this state:

```python
# Get position state
position = event_bus.get_position("XAUUSD", "67890")
print(position)  # {'position_id': '67890', 'status': 'active', ...}

# Get order state
order = event_bus.get_order("XAUUSD", "12345")
print(order)  # {'order_id': '12345', 'status': 'pending', ...}

# Get all active positions for a symbol
positions = event_bus.get_active_positions("XAUUSD")
for pos in positions:
    print(f"Position {pos['position_id']}: {pos['volume']} lots")
```

## Channel Naming Convention

Events are published to channels with the following format:

```
trades:{symbol}:{event_type}
```

Examples:
- `trades:XAUUSD:order.created`
- `trades:BTCUSD:position.opened`
- `trades:XAUUSD:signal.generated`

## Integration with TradeExecutor

The event bus can be injected into the TradeExecutor via ExecutorBuilder:

```python
from app.trader.executor_builder import ExecutorBuilder

# Create event bus
event_bus = EventBusFactory.create_from_env(env_config)

# Build executor with event bus
trade_executor = ExecutorBuilder.build_from_config(
    config=executor_config,
    client=client,
    event_bus=event_bus,  # Inject here
    logger=logger
)
```

The OrderExecutor will then publish events automatically:
- When orders are created
- When orders are filled
- When positions are opened/closed
- When trades succeed or fail

## Error Handling

```python
try:
    event_bus = EventBusFactory.create_from_env(env_config)
except ImportError:
    print("redis-py not installed. Install with: pip install redis")
except redis.ConnectionError:
    print("Cannot connect to Redis. Is the Docker container running?")

# Check connection status
if event_bus.is_connected:
    event_bus.publish(event)
else:
    print("Redis connection lost")
```

## Context Manager

```python
with EventBusFactory.create_from_env(env_config) as event_bus:
    event = TradeEvent.create_order_created(...)
    event_bus.publish(event)
# Connection automatically closed
```

## Requirements

Install redis-py:

```bash
pip install redis
```

## Redis Docker Container Setup

The Redis container should be running separately. Example docker-compose.yml:

```yaml
version: '3.8'
services:
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data
    command: redis-server --appendonly yes

volumes:
  redis-data:
```

Start the container:

```bash
docker-compose up -d redis
```
