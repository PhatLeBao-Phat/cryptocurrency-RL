# ----------------------------------------------
# Define Logging utils
# ----------------------------------------------
from loguru import logger
import functools
from typing import Callable

# Configure Loguru logger
logger.add(
    "logs/log_{time:YYYY-MM-DD}.log",
    level="INFO",
    format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
    diagnose=True,
    rotation="4 days"
)

def log_operation(func: Callable):
    """Log wrapper around functions"""
    @functools.wraps(func)
    def operation_wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            logger.info(f"Executed function {func.__name__}: Returned {type(result)}")
            return result
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}")
            raise
    return operation_wrapper
