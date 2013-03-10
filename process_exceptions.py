#!/usr/bin/env python
#
# Helper module to aid with processing exceptions

import sys
import traceback

# Report last exception as string
def last_exception():
    """ Returns last exception as a string
    """
    exc_type, exc_value, exc_traceback = sys.exc_info()
    return ''.join(traceback.format_exception(exc_type, exc_value, 
                                              exc_traceback))
