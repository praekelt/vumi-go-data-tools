import argparse
import dateutil.parser
from functools import partial

from gdt import codec
from gdt.filters import (FilterPipeline, MSISDNFilter, TimestampFilter,
                         DirectionalFilter, SessionEventFilter)


def make_pipeline(filter_class, kwargs, codec_class):
    return FilterPipeline([filter_class(**kwargs)], codec_class=codec_class)


def dispatch(args):
    subcommand_name = args.pop('subcommand_name')
    codec_class = args.pop('codec_class')

    dispatch_map = {
        'msisdn': partial(make_pipeline, MSISDNFilter),
        'daterange': partial(make_pipeline, TimestampFilter),
        'direction': partial(make_pipeline, DirectionalFilter),
        'session': partial(make_pipeline, SessionEventFilter)
    }

    pipeline = dispatch_map[subcommand_name](args, codec_class)
    pipeline.process()


def get_codec(codec_name):
    return {
        'csv': codec.CSVMessageCodec,
        'json': codec.JSONMessageCodec,
    }.get(codec_name)


def get_parser():

    parser = argparse.ArgumentParser(description='Vumi Go Data Tools')
    parser.add_argument(
        '-c', '--codec', help='Which codec to use.', required=False,
        dest='codec_class', type=get_codec, default=codec.CSVMessageCodec)

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
    msisdn_parser.set_defaults(subcommand_name='msisdn')

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
    daterange_parser.set_defaults(subcommand_name='daterange')

    direction_parser = subparsers.add_parser(
        'direction', help='Filter on message direction.')
    direction_parser.add_argument(
        '-d', '--direction', help='Message direction to extract',
        required=True, choices=['inbound', 'outbound'],
        dest='direction')
    direction_parser.set_defaults(subcommand_name='direction')

    session_parser = subparsers.add_parser(
        'session', help='Filter on session events.')
    session_parser.add_argument(
        '-t', '--event-type', help='The session events to extract.',
        dest='event_type', required=False,
        choices=['new', 'resume', 'end'])
    session_parser.set_defaults(subcommand_name='session')

    return parser
