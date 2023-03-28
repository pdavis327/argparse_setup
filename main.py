"""
This pipeline...
"""

import argparse
import os
from util.argparse import valid_date
from util.argparse import pretty_print_args
from util.argparse import setup_logging
from pipeline import Pipeline
import logging

PIPELINE_VERSION = '0.0.1'
PIPELINE_NAME = 'argparse_setup'
PIPELINE_DESCRIPTION = 'This pipeline is used to test argparse for future pipeline work.'

# Some optional git parameters provided as environment variables.  Used for logging.
COMMIT_SHA = os.getenv('COMMIT_SHA', '')
COMMIT_BRANCH = os.getenv('COMMIT_BRANCH', '')
COMMIT_REPO = os.getenv('COMMIT_REPO', '')


parser = argparse.ArgumentParser(description=f'{PIPELINE_NAME} {PIPELINE_VERSION} - {PIPELINE_DESCRIPTION}')

### Common arguments
parser.add_argument('input_num', type=int,
                    help='input a number to square')

parser.add_argument('-v', '--verbose',
                    action='count',
                    dest='verbosity',
                    default=0,
                    help="verbose output (repeat for increased verbosity)")


args = parser.parse_args()
setup_logging(args.verbosity)
log = logging.getLogger()

args.COMMIT_SHA = COMMIT_SHA
args.COMMIT_BRANCH = COMMIT_BRANCH
args.COMMIT_REPO = COMMIT_REPO

args.PIPELINE_VERSION = PIPELINE_VERSION
args.PIPELINE_NAME = PIPELINE_NAME
args.PIPELINE_DESCRIPTION = PIPELINE_DESCRIPTION

log.info(f'{PIPELINE_NAME} v{PIPELINE_VERSION}')
log.info(f'{PIPELINE_DESCRIPTION}')
log.info(pretty_print_args(args))


pipeline = Pipeline(args)

if __name__ == '__main__':
    result = pipeline.run()

    # exit code=0 indicates success.  Any other value indicates a failure
    exit_code = 0 if result else 1
    exit(exit_code)
