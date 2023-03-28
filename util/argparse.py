"""
Utilities for use with argparse
"""

from datetime import datetime
import argparse
import logging
import os
import sys


def valid_date(s):
    """
    Use with argparse to validate a date parameter

    Example Usage:
    parser.add_argument('--start_date', type=valid_date,
                    help='Start date for the pipeline. '
                         'Format: YYYY-MM-DD (default: %(default)s)',
                    default='2019-01-01')
    """

    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)


def pretty_print_args(args):
    arg_str = '\n'.join(f'  {k}={v}' for k, v in vars(args).items())
    return f'Executing with parameters:\n{arg_str}'


def setup_logging(verbosity):
    base_loglevel = getattr(logging,
                            (os.getenv('LOGLEVEL', 'WARNING')).upper())
    verbosity = min(verbosity, 2)
    loglevel = base_loglevel - (verbosity * 10)
    logging.basicConfig(stream=sys.stdout,
                        level=loglevel,
                        format='%(message)s')
