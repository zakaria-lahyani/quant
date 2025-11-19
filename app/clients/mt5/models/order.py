from dataclasses import dataclass
from typing import Optional


@dataclass
class PendingOrder:
    ticket: int
    symbol: str
    type: int
    price_open: float
    price_current: float
    sl: float
    tp: float
    volume_initial: float
    volume_current: float
    state: int
    time_setup: Optional[int] = None
    time_setup_msc: Optional[int] = None
    comment: Optional[str] = ""
    magic: Optional[int] = 0
    external_id: Optional[str] = ""

    @staticmethod
    def from_dict(data: dict) -> "PendingOrder":
        return PendingOrder(
            ticket=data["ticket"],
            symbol=data["symbol"],
            type=data["type"],
            price_open=data["price_open"],
            price_current=data["price_current"],
            sl=data.get("sl", 0),
            tp=data.get("tp", 0),
            volume_initial=data["volume_initial"],
            volume_current=data["volume_current"],
            state=data["state"],
            time_setup=data.get("time_setup"),
            time_setup_msc=data.get("time_setup_msc"),
            comment=data.get("comment", ""),
            magic=data.get("magic", 0),
            external_id=data.get("external_id", "")
        )