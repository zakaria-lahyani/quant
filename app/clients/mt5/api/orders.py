"""
Orders client for MT5 API.
"""

from typing import Any, Dict, List, Optional

from app.clients.mt5.base import BaseClient
from app.clients.mt5.models.response import (
    Order,
    CreateOrderRequest,
    UpdatePendingOrderRequest,
    DeletePendingOrderRequest,
    OrderType,
)
from app.clients.mt5.utils import validate_symbol, validate_ticket, validate_volume, normalize_volume


class OrdersClient(BaseClient):
    """Client for managing MT5 orders."""

    def create_order(
            self,
            symbol: str,
            volume: float,
            order_type: OrderType,
            price: Optional[float] = None,
            stop_loss: Optional[float] = None,
            take_profit: Optional[float] = None,
            comment: str = "",
            magic: int = 0,
    ) -> Dict[str, Any]:
        """
        Create a new market or pending order.

        Args:
            symbol: Trading symbol (e.g., 'EURUSD')
            volume: Trading volume (must be positive)
            order_type: Order type
            price: Price for pending orders (required for pending orders)
            stop_loss: Stop loss level
            take_profit: Take profit level
            comment: Order comment
            magic: Magic number (identifier)

        Returns:
            Response indicating success or failure of the order creation.
        """
        symbol = validate_symbol(symbol)
        volume = validate_volume(volume)
        volume = normalize_volume(volume)  # Normalize to broker's volume step

        order_data = CreateOrderRequest(
            symbol=symbol,
            volume=volume,
            order_type=order_type,
            price=price,
            sl=stop_loss,
            tp=take_profit,
            comment=comment,
            magic=magic,
        )

        return self.post("orders/create", json_data=order_data.model_dump())

    def create_buy_order(
            self,
            symbol: str,
            volume: float,
            stop_loss: Optional[float] = None,
            take_profit: Optional[float] = None,
            comment: str = "",
            magic: int = 0,
    ) -> Dict[str, Any]:
        """
        Create a market buy order.

        Args:
            symbol: Trading symbol
            volume: Trading volume
            stop_loss: Stop loss level
            take_profit: Take profit level
            comment: Order comment
            magic: Magic number

        Returns:
            Response indicating success or failure.
        """
        return self.create_order(
            symbol=symbol,
            volume=volume,
            order_type=OrderType.BUY,
            stop_loss=stop_loss,
            take_profit=take_profit,
            comment=comment,
            magic=magic,
        )

    def create_sell_order(
            self,
            symbol: str,
            volume: float,
            stop_loss: Optional[float] = None,
            take_profit: Optional[float] = None,
            comment: str = "",
            magic: int = 0,
    ) -> Dict[str, Any]:
        """
        Create a market sell order.

        Args:
            symbol: Trading symbol
            volume: Trading volume
            stop_loss: Stop loss level
            take_profit: Take profit level
            comment: Order comment
            magic: Magic number

        Returns:
            Response indicating success or failure.
        """
        return self.create_order(
            symbol=symbol,
            volume=volume,
            order_type=OrderType.SELL,
            stop_loss=stop_loss,
            take_profit=take_profit,
            comment=comment,
            magic=magic,
        )

    # Limit Orders
    def create_buy_limit_order(
            self,
            symbol: str,
            volume: float,
            price: float,
            stop_loss: Optional[float] = None,
            take_profit: Optional[float] = None,
            comment: str = "",
            magic: int = 0,
    ) -> Dict[str, Any]:
        """
        Create a buy limit order (pending order to buy at or below specified price).

        Args:
            symbol: Trading symbol
            volume: Trading volume
            price: Limit price (must be below current market price for buy limit)
            stop_loss: Stop loss level
            take_profit: Take profit level
            comment: Order comment
            magic: Magic number

        Returns:
            Response indicating success or failure.
        """
        return self.create_order(
            symbol=symbol,
            volume=volume,
            order_type=OrderType.BUY_LIMIT,
            price=price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            comment=comment,
            magic=magic,
        )

    def create_sell_limit_order(
            self,
            symbol: str,
            volume: float,
            price: float,
            stop_loss: Optional[float] = None,
            take_profit: Optional[float] = None,
            comment: str = "",
            magic: int = 0,
    ) -> Dict[str, Any]:
        """
        Create a sell limit order (pending order to sell at or above specified price).

        Args:
            symbol: Trading symbol
            volume: Trading volume
            price: Limit price (must be above current market price for sell limit)
            stop_loss: Stop loss level
            take_profit: Take profit level
            comment: Order comment
            magic: Magic number

        Returns:
            Response indicating success or failure.
        """
        return self.create_order(
            symbol=symbol,
            volume=volume,
            order_type=OrderType.SELL_LIMIT,
            price=price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            comment=comment,
            magic=magic,
        )

    # Stop Orders
    def create_buy_stop_order(
            self,
            symbol: str,
            volume: float,
            price: float,
            stop_loss: Optional[float] = None,
            take_profit: Optional[float] = None,
            comment: str = "",
            magic: int = 0,
    ) -> Dict[str, Any]:
        """
        Create a buy stop order (pending order to buy at or above specified price).

        Args:
            symbol: Trading symbol
            volume: Trading volume
            price: Stop price (must be above current market price for buy stop)
            stop_loss: Stop loss level
            take_profit: Take profit level
            comment: Order comment
            magic: Magic number

        Returns:
            Response indicating success or failure.
        """
        return self.create_order(
            symbol=symbol,
            volume=volume,
            order_type=OrderType.BUY_STOP,
            price=price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            comment=comment,
            magic=magic,
        )

    def create_sell_stop_order(
            self,
            symbol: str,
            volume: float,
            price: float,
            stop_loss: Optional[float] = None,
            take_profit: Optional[float] = None,
            comment: str = "",
            magic: int = 0,
    ) -> Dict[str, Any]:
        """
        Create a sell stop order (pending order to sell at or below specified price).

        Args:
            symbol: Trading symbol
            volume: Trading volume
            price: Stop price (must be below current market price for sell stop)
            stop_loss: Stop loss level
            take_profit: Take profit level
            comment: Order comment
            magic: Magic number

        Returns:
            Response indicating success or failure.
        """
        return self.create_order(
            symbol=symbol,
            volume=volume,
            order_type=OrderType.SELL_STOP,
            price=price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            comment=comment,
            magic=magic,
        )

    # Stop Limit Orders
    def create_buy_stop_limit_order(
            self,
            symbol: str,
            volume: float,
            price: float,
            stop_loss: Optional[float] = None,
            take_profit: Optional[float] = None,
            comment: str = "",
            magic: int = 0,
    ) -> Dict[str, Any]:
        """
        Create a buy stop limit order (becomes a limit order when stop price is reached).

        Args:
            symbol: Trading symbol
            volume: Trading volume
            price: Stop limit price
            stop_loss: Stop loss level
            take_profit: Take profit level
            comment: Order comment
            magic: Magic number

        Returns:
            Response indicating success or failure.
        """
        return self.create_order(
            symbol=symbol,
            volume=volume,
            order_type=OrderType.BUY_STOP_LIMIT,
            price=price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            comment=comment,
            magic=magic,
        )

    def create_sell_stop_limit_order(
            self,
            symbol: str,
            volume: float,
            price: float,
            stop_loss: Optional[float] = None,
            take_profit: Optional[float] = None,
            comment: str = "",
            magic: int = 0,
    ) -> Dict[str, Any]:
        """
        Create a sell stop limit order (becomes a limit order when stop price is reached).

        Args:
            symbol: Trading symbol
            volume: Trading volume
            price: Stop limit price
            stop_loss: Stop loss level
            take_profit: Take profit level
            comment: Order comment
            magic: Magic number

        Returns:
            Response indicating success or failure.
        """
        return self.create_order(
            symbol=symbol,
            volume=volume,
            order_type=OrderType.SELL_STOP_LIMIT,
            price=price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            comment=comment,
            magic=magic,
        )

    def get_pending_orders(self, symbol: Optional[str] = None) -> List[Order]:
        """
        Get all pending orders, optionally filtered by symbol.

        Args:
            symbol: Optional symbol to filter orders by.

        Returns:
            List of pending orders.
        """
        params = {}
        if symbol:
            params['symbol'] = validate_symbol(symbol)

        data = self.get("orders", params=params)
        return [Order(**order) for order in data] if data else []

    def update_pending_order(
            self,
            ticket: int,
            price: Optional[float] = None,
            stop_loss: Optional[float] = None,
            take_profit: Optional[float] = None,
            comment: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Update a pending order.

        Args:
            ticket: Order ticket number
            price: New price
            stop_loss: New stop loss
            take_profit: New take profit
            comment: New comment

        Returns:
            Response indicating success or failure of the update operation.
        """
        ticket = validate_ticket(ticket)

        update_data = UpdatePendingOrderRequest(
            ticket=ticket,
            price=price,
            sl=stop_loss,
            tp=take_profit,
            comment=comment,
        )

        return self.put("orders/update", json_data=update_data.model_dump())

    def delete_pending_order(self, ticket: int) -> Dict[str, Any]:
        """
        Delete a pending order.

        Args:
            ticket: Order ticket number

        Returns:
            Response indicating success or failure of the deletion operation.
        """
        ticket = validate_ticket(ticket)

        delete_data = DeletePendingOrderRequest(ticket=ticket)

        return self.delete("orders/delete", json_data=delete_data.model_dump())

    def cancel_order(self, ticket: int) -> Dict[str, Any]:
        """
        Cancel a pending order (alias for delete_pending_order).

        Args:
            ticket: Order ticket number

        Returns:
            Response indicating success or failure of the cancellation operation.
        """
        return self.delete_pending_order(ticket)

    def delete_all_pending_orders(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """
        Delete all pending orders, optionally filtered by symbol.

        Args:
            symbol: Optional symbol to filter orders by. If None, deletes all pending orders.

        Returns:
            Dictionary with results of deletion operations.
        """
        try:
            # Get all pending orders (filtered by symbol if provided)
            pending_orders = self.get_pending_orders(symbol=symbol)

            results = {
                'total_orders': len(pending_orders),
                'deleted_successfully': 0,
                'failed_deletions': 0,
                'results': []
            }

            for order in pending_orders:
                try:
                    delete_result = self.delete_pending_order(order.ticket)
                    results['results'].append({
                        'ticket': order.ticket,
                        'symbol': order.symbol,
                        'status': 'success',
                        'result': delete_result
                    })
                    results['deleted_successfully'] += 1
                except Exception as e:
                    results['results'].append({
                        'ticket': order.ticket,
                        'symbol': order.symbol,
                        'status': 'failed',
                        'error': str(e)
                    })
                    results['failed_deletions'] += 1

            return results

        except Exception as e:
            return {
                'total_orders': 0,
                'deleted_successfully': 0,
                'failed_deletions': 0,
                'error': str(e),
                'results': []
            }

    def delete_orders_by_symbol(self, symbol: str) -> Dict[str, Any]:
        """
        Delete all pending orders for a specific symbol.

        Args:
            symbol: Trading symbol to delete orders for

        Returns:
            Dictionary with results of deletion operations.
        """
        symbol = validate_symbol(symbol)
        return self.delete_all_pending_orders(symbol=symbol)

    def delete_orders_by_magic(self, magic: int) -> Dict[str, Any]:
        """
        Delete all pending orders with a specific magic number.

        Args:
            magic: Magic number to filter orders by

        Returns:
            Dictionary with results of deletion operations.
        """
        try:
            # Get all pending orders
            pending_orders = self.get_pending_orders()

            # Filter by magic number
            filtered_orders = [order for order in pending_orders if order.magic == magic]

            results = {
                'total_orders': len(filtered_orders),
                'deleted_successfully': 0,
                'failed_deletions': 0,
                'magic_number': magic,
                'results': []
            }

            for order in filtered_orders:
                try:
                    delete_result = self.delete_pending_order(order.ticket)
                    results['results'].append({
                        'ticket': order.ticket,
                        'symbol': order.symbol,
                        'magic': order.magic,
                        'status': 'success',
                        'result': delete_result
                    })
                    results['deleted_successfully'] += 1
                except Exception as e:
                    results['results'].append({
                        'ticket': order.ticket,
                        'symbol': order.symbol,
                        'magic': order.magic,
                        'status': 'failed',
                        'error': str(e)
                    })
                    results['failed_deletions'] += 1

            return results

        except Exception as e:
            return {
                'total_orders': 0,
                'deleted_successfully': 0,
                'failed_deletions': 0,
                'magic_number': magic,
                'error': str(e),
                'results': []
            }

