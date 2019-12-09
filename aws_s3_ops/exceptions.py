"""
This module defines custom exceptions
"""

__author__ = "Aashima Yuthika"
__version__ = "0.0.0"


class UnsupportedCompressionException(Exception):
    """
    This class is mainly for throwing an exception when an compression is encountered
    """
    pass


class S3FileNotFoundException(Exception):
    """
    This class is mainly for throwing an exception when an s3 file is not found
    """
    pass


class LocalFileNotFoundException(Exception):
    """
    This class is mainly for throwing an exception when a local file that is to be uploaded is not found
    """
    pass
