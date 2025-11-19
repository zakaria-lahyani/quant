from enum import Enum


class MT5TimeFrameEnum(str, Enum):
    """Time frame enumeration for trading strategies."""
    M1 = "1"
    M5 = "5"
    M15 = "15"
    M30 = "30"
    H1 = "60"
    H4 = "240"
    D1 = "1d"