# src/exceptions.py
class DataError(Exception):
    """Base class for data-related errors"""
    pass

class DataValidationError(DataError):
    """Raised when data validation fails"""
    pass