"""
Restriction Manager - Handles all trading restrictions (news, market closing, etc.)
"""

import logging
from datetime import datetime
from typing import Optional

from app.trader.trading_context import TradingContext, MarketState
from .suspension_store import SuspensionStore, SuspendedItem
from .trade_restriction import TradeRestriction
from app.trader.live_trader import LiveTrader


class RestrictionManager:
    """
    Manages all trading restrictions and suspensions.
    Single responsibility: Handle trading restrictions.
    """
    
    def __init__(
        self,
        trader: LiveTrader,
        suspension_store: SuspensionStore,
        trade_restriction: TradeRestriction,
        symbol: str,
        account_type: str,
        logger: Optional[logging.Logger] = None
    ):
        self.trader = trader
        self.suspension_store = suspension_store
        self.trade_restriction = trade_restriction
        self.symbol = symbol
        self.account_type = account_type
        self.logger = logger or logging.getLogger(self.__class__.__name__)
        
        # State tracking
        self.last_news_state: Optional[bool] = None
        
    def apply_restrictions(self, context: TradingContext) -> None:
        """
        Apply all trading restrictions to the context.
        
        Args:
            context: Trading context to update
        """
        if not context.current_time or not context.market_state:
            return
            
        # Check and apply news restrictions
        self._apply_news_restrictions(context)
        
        # Check and apply market closing restrictions
        self._apply_market_closing_restrictions(context)
        
    def _apply_news_restrictions(self, context: TradingContext) -> None:
        """Handle news event restrictions."""
        is_news_active = self.trade_restriction.is_news_block_active(context.current_time)
        context.news_block_active = is_news_active
        
        # Detect state transitions
        if self.last_news_state != is_news_active:
            if is_news_active:  # News started
                self._on_news_started(context.market_state)
                context.block_trading("news_event")
            elif self.last_news_state is True:  # News ended
                self._on_news_ended()
                # Only allow trading if no other restrictions
                if not context.market_closing_soon:
                    context.allow_trading()
                    
            self.last_news_state = is_news_active
            
    def _apply_market_closing_restrictions(self, context: TradingContext) -> None:
        """Handle market closing restrictions for daily accounts."""
        is_closing = self.trade_restriction.is_market_closing_soon(
            self.symbol, 
            context.current_time
        )
        context.market_closing_soon = is_closing
        
        if is_closing and self.account_type == "daily":
            self._on_market_closing(context.market_state)
            context.block_trading("market_closing")
        elif not is_closing and self.account_type == "daily":
            # Allow trading if no news restriction
            if not context.news_block_active:
                context.allow_trading()
                
    def _on_news_started(self, market_state: MarketState) -> None:
        """Handle news event start - suspend trading activity."""
        self.logger.warning("News event started - suspending trading activity")
        
        # Suspend pending orders
        for order in market_state.pending_orders:
            if order.symbol == self.symbol:
                self._suspend_order(order)
                
        # Suspend position SL/TP
        for position in market_state.open_positions:
            if position.symbol == self.symbol and (position.sl != 0 or position.tp != 0):
                self._suspend_position_protection(position)
                
        self.logger.info(f"Suspended {self.suspension_store.count()} items")
        
    def _on_news_ended(self) -> None:
        """Handle news event end - restore suspended items."""
        if self.suspension_store.is_empty():
            self.logger.info("No suspended items to restore")
            return
            
        self.logger.info(f"Restoring {self.suspension_store.count()} suspended items")
        
        # Restore pending orders
        for item in self.suspension_store.get_by_kind('pending_order'):
            self._restore_order(item)
            
        # Restore position SL/TP
        for item in self.suspension_store.get_by_kind('position_sl_tp'):
            self._restore_position_protection(item)
            
        self.suspension_store.clear()
        self.logger.info("All suspended items restored")
        
    def _on_market_closing(self, market_state: MarketState) -> None:
        """Handle market closing for daily accounts."""
        self.logger.warning("Market closing soon - executing daily account closure")
        
        # Close all positions
        for position in market_state.open_positions:
            if position.symbol == self.symbol:
                try:
                    result = self.trader.close_open_position(self.symbol, position.ticket)
                    if 'error' not in result:
                        self.logger.info(f"Closed position {position.ticket}")
                    else:
                        self.logger.error(f"Failed to close position {position.ticket}: {result['error']}")
                except Exception as e:
                    self.logger.error(f"Error closing position {position.ticket}: {e}")
                    
        # Cancel all orders
        for order in market_state.pending_orders:
            if order.symbol == self.symbol:
                try:
                    result = self.trader.cancel_pending_orders(order.ticket)
                    if 'error' not in result:
                        self.logger.info(f"Canceled order {order.ticket}")
                    else:
                        self.logger.error(f"Failed to cancel order {order.ticket}: {result['error']}")
                except Exception as e:
                    self.logger.error(f"Error canceling order {order.ticket}: {e}")
                    
        # Clear suspension store for daily closure
        if not self.suspension_store.is_empty():
            self.suspension_store.clear()
            
    def _suspend_order(self, order) -> None:
        """Suspend a pending order."""
        # Use volume_current if available and > 0, otherwise use volume_initial
        volume = order.volume_current if order.volume_current > 0 else getattr(order, 'volume_initial', 0.01)
        
        suspended_item: SuspendedItem = {
            'ticket': order.ticket,
            'kind': 'pending_order',
            'original_sl': order.sl,
            'original_tp': order.tp,
            'symbol': order.symbol,
            'order_type': self._get_order_type_name(order.type),
            'volume': volume,
            'price': order.price_open,
            'magic': order.magic
        }
        
        self.suspension_store.add(suspended_item)
        
        try:
            result = self.trader.cancel_pending_orders(order.ticket)
            if 'error' not in result:
                self.logger.debug(f"Suspended order {order.ticket}")
        except Exception as e:
            self.logger.error(f"Error suspending order {order.ticket}: {e}")
            
    def _suspend_position_protection(self, position) -> None:
        """Suspend position SL/TP."""
        suspended_item: SuspendedItem = {
            'ticket': position.ticket,
            'kind': 'position_sl_tp',
            'original_sl': position.sl if position.sl != 0 else None,
            'original_tp': position.tp if position.tp != 0 else None,
            'symbol': position.symbol,
            'order_type': None,
            'volume': None,
            'price': None,
            'magic': position.magic
        }
        
        self.suspension_store.add(suspended_item)
        
        try:
            # Use 0 to remove SL/TP, not None
            result = self.trader.update_open_position(position.symbol, position.ticket, 0, 0)
            if 'error' not in result:
                self.logger.debug(f"Suspended SL/TP for position {position.ticket}")
        except Exception as e:
            self.logger.error(f"Error suspending position protection {position.ticket}: {e}")
            
    def _restore_order(self, item: SuspendedItem) -> None:
        """Restore a suspended order."""
        try:
            self.logger.info(
                f"Restoring pending order (original ticket: {item['ticket']}) "
                f"({item['order_type']}, {item['volume']}, {item['price']})"
            )
            
            # Recreate the order based on its type
            order_type = item['order_type']
            result = None
            
            if order_type == 'BUY_LIMIT':
                result = self.trader.client.orders.create_buy_limit_order(
                    symbol=item['symbol'],
                    volume=item['volume'],
                    price=item['price'],
                    stop_loss=item['original_sl'],
                    take_profit=item['original_tp'],
                    magic=item.get('magic')
                )
            elif order_type == 'SELL_LIMIT':
                result = self.trader.client.orders.create_sell_limit_order(
                    symbol=item['symbol'],
                    volume=item['volume'],
                    price=item['price'],
                    stop_loss=item['original_sl'],
                    take_profit=item['original_tp'],
                    magic=item.get('magic')
                )
            elif order_type == 'BUY_STOP':
                result = self.trader.client.orders.create_buy_stop_order(
                    symbol=item['symbol'],
                    volume=item['volume'],
                    price=item['price'],
                    stop_loss=item['original_sl'],
                    take_profit=item['original_tp'],
                    magic=item.get('magic')
                )
            elif order_type == 'SELL_STOP':
                result = self.trader.client.orders.create_sell_stop_order(
                    symbol=item['symbol'],
                    volume=item['volume'],
                    price=item['price'],
                    stop_loss=item['original_sl'],
                    take_profit=item['original_tp'],
                    magic=item.get('magic')
                )
            else:
                self.logger.warning(f"Unknown order type: {order_type}, cannot restore")
                return
            
            if result and isinstance(result, bool) and result:
                self.logger.info(f"Successfully restored order (was ticket {item['ticket']})")
            elif result and isinstance(result, dict) and 'error' not in result:
                self.logger.info(f"Successfully restored order (was ticket {item['ticket']})")
            else:
                error_msg = result.get('error', 'Unknown error') if isinstance(result, dict) else 'Failed'
                self.logger.error(f"Failed to restore order {item['ticket']}: {error_msg}")
                
        except Exception as e:
            self.logger.error(f"Error restoring pending order {item['ticket']}: {e}")
        
    def _restore_position_protection(self, item: SuspendedItem) -> None:
        """Restore position SL/TP."""
        try:
            result = self.trader.update_open_position(
                item['symbol'],
                item['ticket'],
                item['original_sl'],
                item['original_tp']
            )
            if 'error' not in result:
                self.logger.debug(f"Restored SL/TP for position {item['ticket']}")
        except Exception as e:
            self.logger.error(f"Error restoring position protection {item['ticket']}: {e}")
            
    def _get_order_type_name(self, order_type: int) -> str:
        """Convert MT5 order type to string."""
        type_map = {
            0: 'BUY',
            1: 'SELL',
            2: 'BUY_LIMIT',
            3: 'SELL_LIMIT',
            4: 'BUY_STOP',
            5: 'SELL_STOP'
        }
        return type_map.get(order_type, f'UNKNOWN_{order_type}')