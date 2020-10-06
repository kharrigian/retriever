
#####################
### Imports
#####################

## Standard Library
from logging import RootLogger

## External
import pytest

## Local
from retriever.util.logging import get_logger

#####################
### Tests
#####################

def test_get_logger():
    """

    """
    ## Initialize a Logger
    try:
        logger = get_logger(level=20)
    except:
        assert False
    ## Check Object Type
    assert isinstance(logger, RootLogger)
    assert logger.level == 20


