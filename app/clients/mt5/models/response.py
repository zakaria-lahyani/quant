"""
Data models for MT5 API Client using Pydantic.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, field_validator, ValidationInfo


class OrderType(str, Enum):
    """Order types supported by MT5."""
    BUY = "BUY"
    SELL = "SELL"
    BUY_LIMIT = "BUY_LIMIT"
    SELL_LIMIT = "SELL_LIMIT"
    BUY_STOP = "BUY_STOP"
    SELL_STOP = "SELL_STOP"
    BUY_STOP_LIMIT = "BUY_STOP_LIMIT"
    SELL_STOP_LIMIT = "SELL_STOP_LIMIT"


class Timeframe(str, Enum):
    """Timeframes supported by MT5."""
    M1 = "M1"
    M5 = "M5"
    M15 = "M15"
    M30 = "M30"
    H1 = "H1"
    H4 = "H4"
    D1 = "D1"
    W1 = "W1"
    MN1 = "MN1"


class APIResponse(BaseModel):
    """Standard API response model."""
    success: bool
    data: Optional[Any] = None
    message: Optional[str] = None
    error_code: Optional[str] = None


class Position(BaseModel):
    """Position model."""
    ticket: int
    symbol: str
    volume: float
    type: int
    price_open: float
    price_current: float
    profit: float
    swap: float
    commission: Optional[float] = 0.0
    comment: str = ""
    magic: int = 0
    time: Optional[datetime] = None
    sl: Optional[float] = None
    tp: Optional[float] = None


class Order(BaseModel):
    """Order model."""
    ticket: int
    symbol: str
    volume: Optional[float] = None  # Make volume optional as API might not always return it
    type: Union[int, str]  # Accept both int and string types
    price_open: Optional[float] = None  # Make optional as API might use different field names
    price_current: Optional[float] = None
    sl: Optional[float] = None
    tp: Optional[float] = None
    comment: str = ""
    magic: int = 0
    time_setup: Optional[datetime] = None
    time_expiration: Optional[datetime] = None

    # Additional fields that might be present in API response
    time_setup_msc: Optional[int] = None
    time_expiration_msc: Optional[int] = None
    type_time: Optional[int] = None
    type_filling: Optional[int] = None
    state: Optional[int] = None
    position_id: Optional[int] = None
    position_by_id: Optional[int] = None
    reason: Optional[int] = None
    volume_initial: Optional[float] = None
    volume_current: Optional[float] = None
    price_stoplimit: Optional[float] = None
    external_id: Optional[str] = ""

    @field_validator('type', mode='before')
    @classmethod
    def convert_type_to_string(cls, v):
        """Convert integer type to string representation."""
        if isinstance(v, int):
            # MT5 order type mappings
            type_mapping = {
                0: "BUY",
                1: "SELL",
                2: "BUY_LIMIT",
                3: "SELL_LIMIT",
                4: "BUY_STOP",
                5: "SELL_STOP",
                6: "BUY_STOP_LIMIT",
                7: "SELL_STOP_LIMIT"
            }
            return type_mapping.get(v, f"UNKNOWN_TYPE_{v}")
        return v

    @field_validator('volume', mode='before')
    @classmethod
    def handle_volume(cls, v, info: ValidationInfo):
        """Handle missing volume by using volume_initial or volume_current."""
        if v is None:
            # Try to get volume from other fields
            values = info.data if info and info.data else {}
            if 'volume_initial' in values:
                return values.get('volume_initial')
            elif 'volume_current' in values:
                return values.get('volume_current')
            # If still None, set a default small volume
            return 0.01
        return v

    @field_validator('price_open', mode='before')
    @classmethod
    def handle_price_open(cls, v, info: ValidationInfo):
        """Handle missing price_open."""
        if v is None:
            # For pending orders, price_open might be 0 or missing
            return 0.0
        return v


class Symbol(BaseModel):
    """Symbol model."""
    name: str
    description: Optional[str] = None
    currency_base: Optional[str] = None
    currency_profit: Optional[str] = None
    currency_margin: Optional[str] = None
    digits: Optional[int] = None
    point: Optional[float] = None
    spread: Optional[int] = None
    trade_mode: Optional[str] = None
    volume_min: Optional[float] = None
    volume_max: Optional[float] = None
    volume_step: Optional[float] = None


class TickInfo(BaseModel):
    """Tick information model."""
    symbol: str
    time: datetime
    bid: float
    ask: float
    last: Optional[float] = None
    volume: Optional[int] = None
    flags: Optional[int] = None


class Account(BaseModel):
    """Account information model."""
    login: int
    trade_mode: str
    name: str
    server: str
    currency: str
    leverage: int
    balance: float
    equity: float
    margin: float
    margin_free: float
    margin_level: Optional[float] = None
    profit: float
    company: Optional[str] = None


class HistoricalBar(BaseModel):
    """Historical price bar model."""
    time: datetime
    open: float
    high: float
    low: float
    close: float
    tick_volume: int
    spread: Optional[int] = None
    real_volume: Optional[int] = None


class HistoricalData(BaseModel):
    """Historical data container."""
    symbol: str
    timeframe: str
    bars: List[HistoricalBar]
    count: int


# Request Models

class CreateOrderRequest(BaseModel):
    """Request model for creating an order."""
    symbol: str = Field(..., description="Trading symbol (e.g., 'EURUSD')")
    volume: float = Field(..., gt=0, description="Trading volume (must be positive)")
    order_type: OrderType = Field(..., description="Order type")
    price: Optional[float] = Field(None, description="Price for pending orders")
    sl: Optional[float] = Field(None, description="Stop loss level")
    tp: Optional[float] = Field(None, description="Take profit level")
    comment: str = Field("", description="Order comment")
    magic: int = Field(0, description="Magic number (identifier)")

    @field_validator('price')
    @classmethod
    def validate_price(cls, v, info: ValidationInfo):
        """Validate price is required for pending orders."""
        values = info.data if info and info.data else {}
        order_type = values.get('order_type')
        if order_type and order_type not in [OrderType.BUY, OrderType.SELL] and v is None:
            raise ValueError("Price is required for pending orders")
        return v


class PositionUpdateRequest(BaseModel):
    """Request model for updating position stop loss or take profit."""
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None


class PositionCloseRequest(BaseModel):
    """Request model for closing a position."""
    volume: float = Field(..., gt=0, description="Volume to close (must be positive)")


class CloseAllPositionsRequest(BaseModel):
    """Request model for closing all positions."""
    symbol: Optional[str] = None


class UpdatePendingOrderRequest(BaseModel):
    """Request model for updating a pending order."""
    ticket: int = Field(..., description="Order ticket number")
    price: Optional[float] = Field(None, description="New price")
    sl: Optional[float] = Field(None, description="New stop loss")
    tp: Optional[float] = Field(None, description="New take profit")
    comment: Optional[str] = Field(None, description="New comment")


class DeletePendingOrderRequest(BaseModel):
    """Request model for deleting a pending order."""
    ticket: int = Field(..., description="Order ticket number")


class SymbolSelectRequest(BaseModel):
    """Request model for selecting a symbol in Market Watch."""
    symbol: str = Field(..., description="Symbol name")
    enable: bool = Field(True, description="True to add to Market Watch, False to remove")


class SymbolSelectResponse(BaseModel):
    """Response model for symbol selection operation."""
    symbol: str = Field(..., description="Symbol name")
    success: bool = Field(..., description="Whether the operation was successful")
    action: str = Field(..., description="Action performed (added/removed)")


class SymbolTradableResponse(BaseModel):
    """Response model for symbol tradability check."""
    symbol: str = Field(..., description="Symbol name")
    is_tradable: bool = Field(..., description="Whether the symbol is tradable")


class ValidationError(BaseModel):
    """Validation error model."""
    loc: List[Union[str, int]]
    msg: str
    type: str


class HTTPValidationError(BaseModel):
    """HTTP validation error model."""
    detail: List[ValidationError]
