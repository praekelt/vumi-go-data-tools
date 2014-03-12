import argparse
import dateutil.parser
from functools import partial

from gdt.filters import (FilterPipeline, MSISDNFilter, TimestampFilter,
                         DirectionalFilter)


def make_pipeline(filter_class, kwargs):
    return FilterPipeline([filter_class(**kwargs)])

dispatch = {
    'msisdn': partial(make_pipeline, MSISDNFilter),
    'daterange': partial(make_pipeline, TimestampFilter),
    'direction': partial(make_pipeline, DirectionalFilter)
}


def get_parser():

    parser = argparse.ArgumentParser(description='Vumi Go Data Tools')

    subparsers = parser.add_subparsers(help='use `command --help`.')

    msisdn_parser = subparsers.add_parser(
        'msisdn', help='Filter on an msisdn')
    msisdn_parser.add_argument(
        '-m', '--msisdn', help='MSISDN to extract messages for',
        dest='msisdn',
        required=True)
    msisdn_parser.add_argument(
        '-t', '--addr-type',
        help='Which address type to filter on.',
        dest='addr_type',
        choices=['to_addr', 'from_addr'], required=True)
    msisdn_parser.set_defaults(dispatch='msisdn')

    daterange_parser = subparsers.add_parser(
        'daterange', help='Filter on a date range.')
    daterange_parser.add_argument(
        '-s', '--start',
        help=('Date time to start from '
              '(as ISO timestamp, e.g. 2013-09-01 01:00:00)'),
        dest='start',
        required=False, type=dateutil.parser.parse)
    daterange_parser.add_argument(
        '-e', '--end',
        help=('Date time to extract to '
              '(as ISO timestamp, e.g. 2013-09-10 03:00:00)'),
        dest='end',
        required=False, type=dateutil.parser.parse)
    daterange_parser.set_defaults(dispatch='daterange')

    direction_parser = subparsers.add_parser(
        'direction', help='Filter on message direction.')
    direction_parser.add_argument(
        '-d', '--direction', help='Message direction to extract',
        required=True, choices=['inbound', 'outbound'],
        dest='direction')
    direction_parser.set_defaults(dispatch='direction')

    return parser
