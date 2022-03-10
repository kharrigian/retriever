
########################
### Imports
########################

## Standard Libarary
import sys
import logging

########################
### Functions
########################

def get_logger(level=logging.WARNING):
    """
    Create a logger object for outputing
    to standard out

    Args:
        level (int or str): Logging filter level
    
    Returns:
        logger (Logging object): Python logger
    """
    logger = logging.getLogger()
    logger.setLevel(level)
    if not logger.hasHandlers():
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        logger.addHandler(handler)
    return logger
