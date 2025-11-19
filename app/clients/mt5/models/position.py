from dataclasses import dataclass
from typing import Optional


@dataclass
class Position:
    ticket: int
    symbol: str
    type: int
    volume: float
    price_open: float
    price_current: float
    sl: float
    tp: float
    profit: float
    swap: float
    magic: int
    comment: str
    reason: int
    time: int
    time_msc: int
    time_update: int
    time_update_msc: int
    external_id: Optional[str] = ""
    identifier: Optional[int] = None

    @classmethod
    def from_dict(cls, data: dict) -> "Position":
        return cls(
            ticket=data.get("ticket"),
            symbol=data.get("symbol"),
            type=data.get("type"),
            volume=data.get("volume"),
            price_open=data.get("price_open"),
            price_current=data.get("price_current"),
            sl=data.get("sl"),
            tp=data.get("tp"),
            profit=data.get("profit"),
            swap=data.get("swap"),
            magic=data.get("magic"),
            comment=data.get("comment"),
            reason=data.get("reason"),
            time=data.get("time"),
            time_msc=data.get("time_msc"),
            time_update=data.get("time_update"),
            time_update_msc=data.get("time_update_msc"),
            external_id=data.get("external_id", ""),
            identifier=data.get("identifier"),
        )
