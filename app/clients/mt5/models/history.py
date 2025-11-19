from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class ClosedPosition:
    ticket: int
    symbol: str
    price: float
    volume: float
    profit: float
    time: datetime
    order: int
    position_id: int
    external_id: str
    type: int
    comment: str
    commission: float
    swap: float
    fee: float
    reason: int
    entry: int
    magic: int
    time_msc: int

    @staticmethod
    def from_dict(data: dict) -> "ClosedPosition":
        return ClosedPosition(
            ticket=data.get("ticket"),
            symbol=data.get("symbol"),
            price=data.get("price"),
            volume=data.get("volume"),
            profit=data.get("profit"),
            time=datetime.strptime(data["time"], "%Y-%m-%d %H:%M:%S"),
            order=data.get("order"),
            position_id=data.get("position_id"),
            external_id=data.get("external_id"),
            type=data.get("type"),
            comment=data.get("comment"),
            commission=data.get("commission"),
            swap=data.get("swap"),
            fee=data.get("fee"),
            reason=data.get("reason"),
            entry=data.get("entry"),
            magic=data.get("magic"),
            time_msc=data.get("time_msc")
        )
