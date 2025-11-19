"""
Custom exceptions for the risk manager package.
"""


class RiskManagerError(Exception):
    """Base exception for all risk manager errors."""
    pass


class InvalidConfigurationError(RiskManagerError):
    """Raised when risk configuration is invalid."""
    
    def __init__(self, message: str, config_type: str = None):
        self.config_type = config_type
        super().__init__(message)


class CalculationError(RiskManagerError):
    """Raised when risk calculations fail."""
    
    def __init__(self, message: str, calculation_type: str = None):
        self.calculation_type = calculation_type
        super().__init__(message)


class ValidationError(RiskManagerError):
    """Raised when input validation fails."""
    
    def __init__(self, message: str, field_name: str = None):
        self.field_name = field_name
        super().__init__(message)


class InsufficientDataError(RiskManagerError):
    """Raised when insufficient market data is available for calculations."""
    
    def __init__(self, message: str, required_data: str = None):
        self.required_data = required_data
        super().__init__(message)


class UnsupportedConfigurationError(RiskManagerError):
    """Raised when a configuration type is not supported."""
    
    def __init__(self, message: str, config_type: str = None):
        self.config_type = config_type
        super().__init__(message)