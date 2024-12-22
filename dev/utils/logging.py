"""
Logging utils
"""
# ----------------------------------------------
# Define Logging utils
# ----------------------------------------------

from loguru import logger
import sys
from typing import Callable

# Config 
logger.add(
    "log/log_{time:YYYY-MM-DD}.log", 
    level="INFO", 
    format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
    diagnose=True,
    rotation="4 days")

# logger.add(
#     sys.stderr,
#     level="INFO", 
#     format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
#     diagnose=True,
# )

def log_operation(func : Callable):
    def operation_wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            logger.info(f"Execute function {func.__name__}: Return {type(result)}")
            return result
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}")
            raise
    return operation_wrapper
        
