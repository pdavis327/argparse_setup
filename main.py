"""
This pipeline generates a list of offsetting ssvid at the segment level.
"""

import argparse
import os
from util.argparse import valid_date
from util.argparse import pretty_print_args
from util.argparse import setup_logging
from pipeline import Pipeline
import logging

PIPELINE_VERSION = '0.0.1'
PIPELINE_NAME = 'offsetting_automation'
PIPELINE_DESCRIPTION = 'This pipeline generates a list of offsetting ssvid at the segment level.'

# Some optional git parameters provided as environment variables.  Used for logging.
COMMIT_SHA = os.getenv('COMMIT_SHA', '')
COMMIT_BRANCH = os.getenv('COMMIT_BRANCH', '')
COMMIT_REPO = os.getenv('COMMIT_REPO', '')


parser = argparse.ArgumentParser(description=f'{PIPELINE_NAME} {PIPELINE_VERSION} - {PIPELINE_DESCRIPTION}')

### Common arguments
parser.add_argument('--test', action='store_true',
                    help='Test mode - print query and exit', default=False)

parser.add_argument('-v', '--verbose',
                    action='count',
                    dest='verbosity',
                    default=0,
                    help="verbose output (repeat for increased verbosity)")

parser.add_argument('-q', '--quiet',
                    action='store_const',
                    const=-1,
                    default=0,
                    dest='verbosity',
                    help="quiet output (show errors only)")

parser.add_argument('--project', type=str,
                    help='GCP project id (default: %(default)s)',
                    default='world-fishing-827')

parser.add_argument('--start_date', type=valid_date,
                    help='Start date. '
                         'Format: YYYY-MM-DD (default: %(default)s)',
                    default='2021-01-01')

parser.add_argument('--end_date', type=valid_date,
                    help='End date. '
                         'Format: YYYY-MM-DD (default: %(default)s)',
                    default='2021-12-31')

### operations
subparsers = parser.add_subparsers(dest='operation', required=True)
satellite_timing = subparsers.add_parser('fishing_hours', help="Create the satellite timing table")
daily_segs = subparsers.add_parser('satellite_timing', help="Create the daily segs table")
aggregate_segs = subparsers.add_parser('aggregate_segs', help="Create the daily segs aggregate table")
validate_args = subparsers.add_parser('validate', help="Validate the output table")

satellite_timing.add_argument('--dest_satellite_timing_table', type=str,
                    help='Destination table for fishing hours by flag state (default: %(default)s)',
                    default='world-fishing-827.scratch_public_ttl120.offestting_satellite_timing')

satellite_timing.add_argument('--messages_scored_table', type=str,
                    help='Input table - messages scored (default: %(default)s)',
                    default='world-fishing-827.pipe_production_v20201001.messages_scored_*')

satellite_timing.add_argument('--research_messages_table', type=str,
                    help='Input table - research messages (default: %(default)s)',
                    default='world-fishing-827.pipe_production_v20201001.research_messages')

satellite_timing.add_argument('--norad_receiver_lookup', type=str,
                    help='Input table - norad to receiver lookup (default: %(default)s)',
                    default='world-fishing-827.gfw_research_precursors.norad_to_receiver_v20200127')

satellite_timing.add_argument('--satellite_positions_table', type=str,
                    help='Input table - satellite positions (default: %(default)s)',
                    default='world-fishing-827.satellite_positions_v20190208.satellite_positions_one_second_resolution_*')

daily_segs.add_argument('--dest_segs_daily_table', type=str,
                    help='Destination table for segments daily (default: %(default)s)',
                    default='world-fishing-827.scratch_public_ttl120.offsetting_segs_daily')

aggregate_segs.add_argument('--dest_agg_segs_table', type=str,
                    help='Destination table for the aggregate of segs daily table (default: %(default)s)',
                    default='world-fishing-827.scratch_public_ttl120.offsetting_agg_segs_daily')


args = parser.parse_args()
setup_logging(args.verbosity)
log = logging.getLogger()

args.COMMIT_SHA = COMMIT_SHA
args.COMMIT_BRANCH = COMMIT_BRANCH
args.COMMIT_REPO = COMMIT_REPO

log.info(f'{PIPELINE_NAME} v{PIPELINE_VERSION}')
log.info(f'{PIPELINE_DESCRIPTION}')
log.info(pretty_print_args(args))

args.PIPELINE_VERSION = PIPELINE_VERSION
args.PIPELINE_NAME = PIPELINE_NAME
args.PIPELINE_DESCRIPTION = PIPELINE_DESCRIPTION

pipeline = Pipeline(args)

if __name__ == '__main__':
    result = pipeline.run()

    # exit code=0 indicates success.  Any other value indicates a failure
    exit_code = 0 if result else 1
    exit(exit_code)
