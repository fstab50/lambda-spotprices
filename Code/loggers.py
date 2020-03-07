"""
Example usage:

>>> __version__ = '1.2.3' # Or wherever the version is stored
>>> logger = _getLogger(__version__)
>>> logger.warn('omg what is happening')
<doctest autotag.loggers[2]> - 1.2.3 - [WARNING]: omg what is happening
"""
import logging
from _version import __version__
import sys


def _getLogger(*args, **kwargs):
    """ custom format logger """
    logger = logging.getLogger(*args, **kwargs)
    logger.propagate = False
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter('%(pathname)s - %(name)s - [%(levelname)s]: %(message)s'))
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
    return logger


def getLogger(*a, **kv):
    return _getLogger(__version__)
