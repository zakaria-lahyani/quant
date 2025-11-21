from typing import Optional, List, Dict, Any
import logging
import time

from app.clients.mt5.client import MT5Client
from app.clients.mt5.models.history import ClosedPosition
from app.clients.mt5.models.order import PendingOrder
from app.clients.mt5.models.response import Position, Order

from app.trader.base_trader import BaseTrader
from app.trader.risk_manager.models import RiskEntryResult
from app.utils.functions_helper import generate_magic_number


class LiveTrader(BaseTrader):
    def __init__(self, client: MT5Client, logger: Optional[logging.Logger] = None, order_delay_seconds: float = 0.5):
        """
        Initialize LiveTrader with MT5 API clients.

        Args:
            client: MT5Client instance for trading operations
            logger: Optional logger for debugging
            order_delay_seconds: Delay in seconds between submitting orders in a split position (default: 0.5)
        """
        super().__init__()
        self.client = client
        self.logger = logger or logging.getLogger(self.__class__.__name__)
        self.order_delay_seconds = order_delay_seconds

    def get_current_price(self, symbol:str):
        price = self.client.symbols.get_symbol_price(symbol)
        return price["bid"]

    def open_pending_order(self, trade: RiskEntryResult) -> List[Dict[str, Any]]:
        """
        Open multiple pending orders from a RiskEntryResult.
        Orders are submitted sequentially with a delay between each submission.

        Args:
            trade: RiskEntryResult containing multiple limit orders

        Returns:
            List of responses from order creation
        """
        results = []

        print(trade)
        print(trade.limit_orders)

        total_orders = len(trade.limit_orders)
        self.logger.info(
            f"Submitting {total_orders} orders sequentially "
            f"(delay: {self.order_delay_seconds}s between orders)"
        )

        # Determine comment prefix based on signal source
        comment_prefix = "Manual_" if getattr(trade, 'is_manual', False) else "Group_"
        order_comment = f"{comment_prefix}{trade.group_id[:8]}"

        for idx, order in enumerate(trade.limit_orders, start=1):
            try:
                # Determine order creation method based on order type
                if order['order_type'] == 'BUY_LIMIT':
                    result = self.client.orders.create_buy_limit_order(
                        symbol=order['symbol'],
                        volume=order['volume'],
                        price=order['price'],
                        stop_loss=order.get('group_stop_loss'),  # Use group stop loss
                        take_profit=trade.take_profit.level if trade.take_profit else None,
                        comment=order_comment,
                        magic=order.get('magic')  # Use the magic number from the order
                    )
                elif order['order_type'] == 'SELL_LIMIT':
                    result = self.client.orders.create_sell_limit_order(
                        symbol=order['symbol'],
                        volume=order['volume'],
                        price=order['price'],
                        stop_loss=order.get('group_stop_loss'),  # Use group stop loss
                        take_profit=trade.take_profit.level if trade.take_profit else None,
                        comment=order_comment,
                        magic=order.get('magic')  # Use the magic number from the order
                    )
                elif order['order_type'] == 'BUY_STOP':
                    result = self.client.orders.create_buy_stop_order(
                        symbol=order['symbol'],
                        volume=order['volume'],
                        price=order['price'],
                        stop_loss=order.get('group_stop_loss'),
                        take_profit=trade.take_profit.level if trade.take_profit else None,
                        comment=order_comment,
                        magic=order.get('magic')  # Use the magic number from the order
                    )
                elif order['order_type'] == 'SELL_STOP':
                    result = self.client.orders.create_sell_stop_order(
                        symbol=order['symbol'],
                        volume=order['volume'],
                        price=order['price'],
                        stop_loss=order.get('group_stop_loss'),
                        take_profit=trade.take_profit.level if trade.take_profit else None,
                        comment=order_comment,
                        magic=order.get('magic')  # Use the magic number from the order
                    )
                else:
                    self.logger.warning(f"Unknown order type: {order['order_type']}")
                    continue

                results.append(result)
                self.logger.info(
                    f"✓ [{idx}/{total_orders}] Created {order['order_type']} order for {order['symbol']} "
                    f"at {order['price']} with volume {order['volume']}"
                )

                # Add delay between orders (except after the last order)
                if idx < total_orders and self.order_delay_seconds > 0:
                    self.logger.debug(f"Waiting {self.order_delay_seconds}s before next order...")
                    time.sleep(self.order_delay_seconds)

            except Exception as e:
                self.logger.error(f"Failed to create order {idx}/{total_orders}: {e}")
                results.append({"error": str(e), "order": order})

        return results

    def get_pending_orders(self, symbol: Optional[str] = None) -> List[PendingOrder]:
        """
        Get all pending orders, optionally filtered by symbol.
        
        Args:
            symbol: Optional symbol to filter orders
            
        Returns:
            List of pending orders
        """
        try:
            orders = self.client.orders.get_pending_orders(symbol)
            self.logger.debug(f"Retrieved {len(orders)} pending orders")
            
            # Convert Order objects to PendingOrder objects
            pending_orders = []
            for order in orders:
                # Get volume, preferring volume_current, then volume_initial, then volume
                volume_current = getattr(order, 'volume_current', None) or getattr(order, 'volume', None) or 0.01
                volume_initial = getattr(order, 'volume_initial', None) or volume_current
                
                pending_order = PendingOrder(
                    ticket=order.ticket,
                    symbol=order.symbol,
                    type=order.type if isinstance(order.type, int) else self._get_order_type_int(str(order.type)),
                    price_open=order.price_open or 0.0,
                    price_current=order.price_current or order.price_open or 0.0,
                    sl=order.sl or 0.0,
                    tp=order.tp or 0.0,
                    volume_initial=volume_initial,
                    volume_current=volume_current,
                    state=0,  # Default state, adjust if needed
                    magic=order.magic if hasattr(order, 'magic') else 0,
                    comment=order.comment or ""
                )
                pending_orders.append(pending_order)
            
            return pending_orders
        except Exception as e:
            self.logger.error(f"Failed to get pending orders: {e}")
            return []

    def update_pending_orders(
        self, 
        ticket: int, 
        price: Optional[float] = None, 
        sl: Optional[float] = None, 
        tp: Optional[float] = None,
        comment: str = ""
    ) -> Dict[str, Any]:
        """
        Update a pending order's parameters.
        
        Args:
            ticket: Order ticket ID
            price: New price level
            sl: New stop loss
            tp: New take profit
            comment: New comment
            
        Returns:
            Response from the update operation
        """
        try:
            result = self.client.orders.update_pending_order(
                ticket=ticket,
                price=price,
                stop_loss=sl,
                take_profit=tp,
                comment=comment
            )
            self.logger.info(f"Updated pending order {ticket}")
            return result
        except Exception as e:
            self.logger.error(f"Failed to update pending order {ticket}: {e}")
            return {"error": str(e)}

    def cancel_pending_orders(self, ticket: int) -> Dict[str, Any]:
        """
        Cancel a specific pending order.
        
        Args:
            ticket: Order ticket ID to cancel
            
        Returns:
            Response from the cancellation
        """
        try:
            result = self.client.orders.delete_pending_order(ticket)
            self.logger.info(f"Cancelled pending order {ticket}")
            # Convert boolean result to dictionary format
            if isinstance(result, bool):
                if result:
                    return {"success": True}
                else:
                    return {"error": "Failed to cancel order"}
            return result
        except Exception as e:
            self.logger.error(f"Failed to cancel pending order {ticket}: {e}")
            return {"error": str(e)}

    def _get_order_type_int(self, order_type_str: str) -> int:
        """
        Convert order type string to integer.
        
        Args:
            order_type_str: Order type as string
            
        Returns:
            Order type as integer
        """
        type_map = {
            'BUY': 0,
            'SELL': 1,
            'BUY_LIMIT': 2,
            'SELL_LIMIT': 3,
            'BUY_STOP': 4,
            'SELL_STOP': 5,
            'BUY_STOP_LIMIT': 6,
            'SELL_STOP_LIMIT': 7
        }
        return type_map.get(order_type_str.upper(), -1)
    
    def cancel_all_pending_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Cancel all pending orders, optionally filtered by symbol.
        
        Args:
            symbol: Optional symbol to filter orders to cancel
            
        Returns:
            List of cancellation results
        """
        results = []
        try:
            orders = self.get_pending_orders(symbol)
            for order in orders:
                result = self.cancel_pending_orders(order.ticket)
                results.append(result)
            
            self.logger.info(f"Cancelled {len(results)} pending orders")
            return results
        except Exception as e:
            self.logger.error(f"Failed to cancel all pending orders: {e}")
            return [{"error": str(e)}]

    def get_open_positions(self, symbol: Optional[str] = None) -> List[Position]:
        """
        Get all open positions, optionally filtered by symbol.
        
        Args:
            symbol: Optional symbol to filter positions
            
        Returns:
            List of open positions
        """
        try:
            if symbol:
                positions = self.client.positions.get_positions_by_symbol(symbol)
            else:
                positions = self.client.positions.get_open_positions()
            
            self.logger.debug(f"Retrieved {len(positions)} open positions")
            return positions
        except Exception as e:
            self.logger.error(f"Failed to get open positions: {e}")
            return []

    def update_open_position(
        self, 
        symbol: str, 
        ticket: int, 
        sl: Optional[float] = None, 
        tp: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Update stop loss or take profit of an open position.
        
        Args:
            symbol: Trading symbol
            ticket: Position ticket ID
            sl: New stop loss level
            tp: New take profit level
            
        Returns:
            Response from the update operation
        """
        try:
            result = self.client.positions.modify_position(
                symbol=symbol,
                ticket=ticket,
                stop_loss=sl,
                take_profit=tp
            )
            self.logger.info(f"Updated position {ticket} for {symbol}")
            # Convert boolean result to dictionary format
            if isinstance(result, bool):
                if result:
                    return {"success": True}
                else:
                    return {"error": "Failed to update position"}
            return result
        except Exception as e:
            self.logger.error(f"Failed to update position {ticket}: {e}")
            return {"error": str(e)}

    def close_open_position(
        self, 
        symbol: str, 
        ticket: int, 
        volume: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Close a specific open position.
        
        Args:
            symbol: Trading symbol
            ticket: Position ticket ID
            volume: Volume to close (if None, closes entire position)
            
        Returns:
            Response from the close operation
        """
        try:
            # If volume not specified, get the position to close full volume
            if volume is None:
                position = self.client.positions.get_position_by_ticket(ticket)
                if position:
                    volume = position.volume
                else:
                    return {"error": f"Position {ticket} not found"}
            
            result = self.client.positions.close_position(
                symbol=symbol,
                ticket=ticket,
                volume=volume
            )
            self.logger.info(f"Closed position {ticket} for {symbol}")
            # Convert boolean result to dictionary format
            if isinstance(result, bool):
                if result:
                    return {"success": True}
                else:
                    return {"error": "Failed to close position"}
            return result
        except Exception as e:
            self.logger.error(f"Failed to close position {ticket}: {e}")
            return {"error": str(e)}

    def close_all_open_position(self) -> Dict[str, Any]:
        """
        Close all open positions.
        
        Returns:
            Response from the close all operation
        """
        try:
            result = self.client.positions.close_all_positions()
            self.logger.info("Closed all positions")
            # Convert boolean result to dictionary format
            if isinstance(result, bool):
                if result:
                    return {"success": True}
                else:
                    return {"error": "Failed to close all positions"}
            return result
        except Exception as e:
            self.logger.error(f"Failed to close all positions: {e}")
            return {"error": str(e)}

    def get_closed_positions(self, start: str, end: str) -> List[ClosedPosition]:
        """
        Get historical closed positions within a date range.
        
        Args:
            start: Start date (format: 'YYYY-MM-DD')
            end: End date (format: 'YYYY-MM-DD')
            
        Returns:
            List of closed positions
        """
        try:
            # Get positions as dictionaries from the API
            positions_data = self.client.history.get_closed_positions(start, end)
            
            # Convert dictionaries to ClosedPosition objects
            positions = []
            for pos_dict in positions_data:
                try:
                    # Handle the time field properly - it might already be a datetime object
                    if 'time' in pos_dict and isinstance(pos_dict['time'], str):
                        # Parse the datetime string if it's a string
                        pos_dict['time'] = pos_dict['time']
                    
                    # Create ClosedPosition object
                    position = ClosedPosition(
                        ticket=pos_dict.get('ticket', 0),
                        symbol=pos_dict.get('symbol', ''),
                        price=pos_dict.get('price', 0.0),
                        volume=pos_dict.get('volume', 0.0),
                        profit=pos_dict.get('profit', 0.0),
                        time=pos_dict.get('time'),
                        order=pos_dict.get('order', 0),
                        position_id=pos_dict.get('position_id', 0),
                        external_id=pos_dict.get('external_id', ''),
                        type=pos_dict.get('type', 0),
                        comment=pos_dict.get('comment', ''),
                        commission=pos_dict.get('commission', 0.0),
                        swap=pos_dict.get('swap', 0.0),
                        fee=pos_dict.get('fee', 0.0),
                        reason=pos_dict.get('reason', 0),
                        entry=pos_dict.get('entry', 0),
                        magic=pos_dict.get('magic', 0),
                        time_msc=pos_dict.get('time_msc', 0)
                    )
                    positions.append(position)
                except Exception as e:
                    self.logger.warning(f"Failed to parse closed position: {e}, data: {pos_dict}")
                    continue
            
            self.logger.debug(
                f"Retrieved {len(positions)} closed positions from {start} to {end}"
            )
            return positions
        except Exception as e:
            self.logger.error(f"Failed to get closed positions: {e}")
            return []

    def get_account_info(self) -> Dict[str, Any]:
        """
        Get current account information.
        
        Returns:
            Account information including balance, equity, margin, etc.
        """
        try:
            info = self.client.account.get_account_info()
            return info
        except Exception as e:
            self.logger.error(f"Failed to get account info: {e}")
            return {}