"""
Core enums and constants for the strategy engine.
"""

from enum import Enum


class TimeFrameEnum(str, Enum):
    """Time frame enumeration for trading strategies."""
    M1 = "1"
    M5 = "5"
    M15 = "15"
    M30 = "30"
    H1 = "60"
    H4 = "240"
    D1 = "1d"


class DaysEnum(str, Enum):
    """Days of the week enumeration."""
    MON = "mon"
    TUE = "tue"
    WED = "wed"
    THU = "thu"
    FRI = "fri"
    SAT = "sat"
    SUN = "sun"


class ConditionOperatorEnum(str, Enum):
    """Condition operators for strategy rules."""
    EQ = "=="
    NE = "!="
    LT = "<"
    LTE = "<="
    GT = ">"
    GTE = ">="
    CROSSES_ABOVE = "crosses_above"
    CROSSES_BELOW = "crosses_below"
    IN = "in"
    NOT_IN = "not_in"
    CHANGES_TO = "changes_to"
    REMAINS = "remains"


class LogicModeEnum(str, Enum):
    """Logic modes for combining conditions."""
    ALL = "all"
    ANY = "any"
    COMPLEX = "complex"


class SessionHandlingEnum(str, Enum):
    """Session handling strategies."""
    CLOSE_ALL = "close_all"
    HOLD = "hold"
    PARTIAL_CLOSE = "partial_close"


class PositionSizingTypeEnum(str, Enum):
    """Position sizing types."""
    FIXED = "fixed"
    PERCENTAGE = "percentage"
    VOLATILITY = "volatility"