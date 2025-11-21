"""
Trader models for position scaling and management.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import List, Optional, Dict, Any
from decimal import Decimal

from app.strategy_builder.data.dtos import EntryDecision, ExitDecision, StopLossResult, TakeProfitResult


class TradeState(Enum):
    """State of a trade or position."""
    PENDING = "pending"           # Order placed but not filled
    ACTIVE = "active"            # Position is open
    PARTIAL_FILLED = "partial"   # Partially filled order
    CLOSED = "closed"            # Position closed
    CANCELLED = "cancelled"      # Order cancelled
    FAILED = "failed"           # Order failed


class PositionType(Enum):
    """Type of position entry."""
    INITIAL = "initial"          # First entry
    SCALE_IN = "scale_in"       # Additional entries (scaling in)
    SCALE_OUT = "scale_out"     # Partial exits


@dataclass
class ScaledPosition:
    """Represents a single scaled position entry."""
    
    # Basic position info
    position_id: str
    group_id: str
    symbol: str
    direction: str  # 'long' or 'short'
    
    # Position details
    entry_price: float
    position_size: float
    target_price: Optional[float] = None
    
    # State management
    state: TradeState = TradeState.PENDING
    position_type: PositionType = PositionType.INITIAL
    
    # Timestamps
    created_at: datetime = field(default_factory=datetime.now)
    filled_at: Optional[datetime] = None
    closed_at: Optional[datetime] = None
    
    # Execution details
    filled_size: float = 0.0
    filled_price: Optional[float] = None
    actual_stop_loss: Optional[float] = None
    
    # Risk management
    stop_loss_level: Optional[float] = None
    take_profit_targets: List[Dict[str, Any]] = field(default_factory=list)
    
    # Metadata
    strategy_name: Optional[str] = None
    magic_number: Optional[int] = None
    notes: str = ""
    
    @property
    def is_filled(self) -> bool:
        """Check if position is completely filled."""
        return self.state == TradeState.ACTIVE and self.filled_size >= self.position_size
    
    @property
    def fill_percentage(self) -> float:
        """Get fill percentage."""
        if self.position_size == 0:
            return 0.0
        return (self.filled_size / self.position_size) * 100
    
    @property
    def unrealized_pnl(self) -> Optional[float]:
        """Calculate unrealized PnL (requires current price)."""
        # This would need current market price to calculate
        # Implemented in position manager with market data
        return None
    
    def update_fill(self, filled_size: float, filled_price: float) -> None:
        """Update position with fill information."""
        self.filled_size += filled_size
        self.filled_price = filled_price
        self.filled_at = datetime.now()
        
        if self.filled_size >= self.position_size:
            self.state = TradeState.ACTIVE
        else:
            self.state = TradeState.PARTIAL_FILLED


@dataclass
class PositionGroup:
    """Manages a group of scaled positions for the same strategy signal."""
    
    # Group identification
    group_id: str
    symbol: str
    strategy_name: str
    direction: str
    
    # Original trade decision
    original_decision: EntryDecision
    
    # Scaling configuration
    total_target_size: float
    num_entries: int
    scaling_strategy: str = "equal"  # 'equal', 'pyramid', 'custom'
    
    # Positions in this group
    positions: List[ScaledPosition] = field(default_factory=list)
    
    # Risk management
    group_stop_loss: Optional[float] = None
    group_take_profit: Optional[TakeProfitResult] = None
    total_risk_amount: float = 0.0
    
    # State tracking
    created_at: datetime = field(default_factory=datetime.now)
    last_entry_at: Optional[datetime] = None
    
    # Performance tracking
    total_filled_size: float = 0.0
    average_entry_price: float = 0.0
    realized_pnl: float = 0.0
    
    @property
    def is_fully_filled(self) -> bool:
        """Check if all positions in group are filled."""
        return all(pos.is_filled for pos in self.positions)
    
    @property
    def active_positions(self) -> List[ScaledPosition]:
        """Get all active positions."""
        return [pos for pos in self.positions if pos.state == TradeState.ACTIVE]
    
    @property
    def pending_positions(self) -> List[ScaledPosition]:
        """Get all pending positions."""
        return [pos for pos in self.positions if pos.state == TradeState.PENDING]
    
    @property
    def fill_percentage(self) -> float:
        """Get overall fill percentage for the group."""
        if self.total_target_size == 0:
            return 0.0
        return (self.total_filled_size / self.total_target_size) * 100
    
    def add_position(self, position: ScaledPosition) -> None:
        """Add a position to the group."""
        position.group_id = self.group_id
        self.positions.append(position)
    
    def update_group_metrics(self) -> None:
        """Update group-level metrics based on positions."""
        active_positions = self.active_positions
        
        if active_positions:
            # Calculate average entry price weighted by position size
            total_value = sum(pos.filled_size * pos.filled_price for pos in active_positions if pos.filled_price)
            total_size = sum(pos.filled_size for pos in active_positions)
            
            if total_size > 0:
                self.average_entry_price = total_value / total_size
                self.total_filled_size = total_size
    
    def calculate_unrealized_pnl(self, current_price: float) -> float:
        """Calculate total unrealized PnL for the group."""
        total_pnl = 0.0
        
        for pos in self.active_positions:
            if pos.filled_price:
                if self.direction.lower() == 'long':
                    pnl = (current_price - pos.filled_price) * pos.filled_size
                else:
                    pnl = (pos.filled_price - current_price) * pos.filled_size
                total_pnl += pnl
        
        return total_pnl
    
    def get_position_summary(self) -> Dict[str, Any]:
        """Get summary of all positions in the group."""
        return {
            'group_id': self.group_id,
            'symbol': self.symbol,
            'strategy': self.strategy_name,
            'direction': self.direction,
            'total_positions': len(self.positions),
            'active_positions': len(self.active_positions),
            'pending_positions': len(self.pending_positions),
            'total_target_size': self.total_target_size,
            'total_filled_size': self.total_filled_size,
            'fill_percentage': self.fill_percentage,
            'average_entry_price': self.average_entry_price,
            'realized_pnl': self.realized_pnl,
            'created_at': self.created_at,
            'last_entry_at': self.last_entry_at
        }


@dataclass
class RiskEntryResult:
    """Result from processing an entry signal with risk management."""

    group_id: str
    limit_orders: List[Dict[str, Any]]
    total_orders: int
    total_size: float
    scaled_sizes: List[float]
    entry_prices: List[float]
    stop_losses: List[Optional[float]]
    group_stop_loss: Optional[float]
    stop_loss_mode: str  # 'group' or 'individual'
    original_risk: float
    take_profit: Optional[Any]  # Can be TakeProfitResult or similar
    calculated_risk: float
    weighted_avg_entry: Optional[float]
    stop_calculation_method: str  # 'monetary' or 'price_level'
    strategy_name: Optional[str] = None
    magic: Optional[int] = None
    is_manual: bool = False  # True for manual signals, prefixes comment with "Manual_"


@dataclass
class ScalingConfig:
    """Configuration for position scaling strategy."""
    
    num_entries: int = 4
    scaling_type: str = "equal"  # 'equal', 'pyramid_up', 'pyramid_down', 'custom'
    entry_spacing: float = 0.5   # Percentage spacing between entries
    max_risk_per_group: float = 500.0  # Maximum total risk for scaled position
    
    # Custom scaling ratios (if scaling_type = 'custom')
    custom_ratios: Optional[List[float]] = None
    
    # Entry timing
    immediate_first_entry: bool = True
    entry_delay_seconds: int = 0
    
    def validate(self) -> bool:
        """Validate scaling configuration."""
        if self.num_entries < 1:
            return False
        
        if self.scaling_type == "custom" and self.custom_ratios:
            if len(self.custom_ratios) != self.num_entries:
                return False
            if abs(sum(self.custom_ratios) - 1.0) > 0.01:  # Should sum to 1.0
                return False
        
        return True
    
    def get_size_ratios(self) -> List[float]:
        """Get size ratios for each entry based on scaling type."""
        if self.scaling_type == "equal":
            ratio = 1.0 / self.num_entries
            return [ratio] * self.num_entries
        
        elif self.scaling_type == "pyramid_up":
            # Smaller first, larger later: [0.1, 0.2, 0.3, 0.4]
            total = sum(range(1, self.num_entries + 1))
            return [i / total for i in range(1, self.num_entries + 1)]
        
        elif self.scaling_type == "pyramid_down":
            # Larger first, smaller later: [0.4, 0.3, 0.2, 0.1]
            total = sum(range(1, self.num_entries + 1))
            return [(self.num_entries - i) / total for i in range(self.num_entries)]
        
        elif self.scaling_type == "custom" and self.custom_ratios:
            return self.custom_ratios.copy()
        
        else:
            # Default to equal
            ratio = 1.0 / self.num_entries
            return [ratio] * self.num_entries