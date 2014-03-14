import argparse
import dateutil.parser
from functools import partial

from gdt import codec
from gdt.filters import (FilterPipeline, MSISDNFilter, TimestampFilter,
                         DirectionalFilter, SessionEventFilter, ContactFilter,
                         RegexFilter, WeekFilter)
from gdt.extractors import ExtractorPipeline, FieldExtractor
from gdt.aggregators import (AggregatorPipeline, UniquesAggregator,
                             SimpleAggregator)


def make_pipeline(filter_class, kwargs, codec_class):
    return FilterPipeline([filter_class(**kwargs)], codec_class=codec_class)


def make_extractor(extractor_class, kwargs, codec_class):
    return ExtractorPipeline([extractor_class(**kwargs)],
                             codec_class=codec_class)


def make_aggregator(aggregator_class, kwargs, codec_class):
    return AggregatorPipeline(aggregator_class(**kwargs),
                              codec_class=codec_class)


def dispatch(args):
    subcommand_name = args.pop('subcommand_name')
    codec_class = args.pop('codec_class')

    dispatch_map = {
        'msisdn': partial(make_pipeline, MSISDNFilter),
        'daterange': partial(make_pipeline, TimestampFilter),
        'weekrange': partial(make_pipeline, WeekFilter),
        'direction': partial(make_pipeline, DirectionalFilter),
        'session': partial(make_pipeline, SessionEventFilter),
        'contacts': partial(make_pipeline, ContactFilter),
        'regex': partial(make_pipeline, RegexFilter),
        'extract': partial(make_extractor, FieldExtractor),
        'aggregate': partial(make_aggregator, UniquesAggregator),
        'count': partial(make_aggregator, SimpleAggregator),
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

    weekrange_parser = subparsers.add_parser(
        'weekrange', help='Filter on a week range.')
    weekrange_parser.add_argument(
        '-y', '--year', help='The year to extract week(s) from',
        dest='year', type=int, required=False)
    weekrange_parser.add_argument(
        '-w', '--weeks', help='The week(s) to extract',
        dest='weeks', type=int, required=False, nargs='+')
    weekrange_parser.set_defaults(subcommand_name='weekrange')

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

    contact_parser = subparsers.add_parser(
        'contacts', help='Filter for certain contact addresses',
        fromfile_prefix_chars='@')
    contact_parser.add_argument(
        '-a', '--address', required=False, dest='addresses', nargs='+')
    contact_parser.set_defaults(subcommand_name='contacts')

    regex_parser = subparsers.add_parser(
        'regex', help='Filter for regex matches')
    regex_parser.add_argument(
        '-f', '--field', required=True, dest='field')
    regex_parser.add_argument(
        '-p', '--pattern', required=True, dest='pattern')
    regex_parser.add_argument(
        '-i', '--ignore-case', required=False, dest='ignore_case',
        action='store_const', const=True, default=False)
    regex_parser.set_defaults(subcommand_name='regex')

    extractor_parser = subparsers.add_parser(
        'extract', help='Extract fields.')
    extractor_parser.add_argument(
        '-f', '--field', help='The field to extract.',
        dest='fields', required=True, nargs='+')
    extractor_parser.add_argument(
        '-df', '--date-format',
        help='`strftime` formatting to apply to the timestamp.',
        dest='date_format', required=True)
    extractor_parser.set_defaults(subcommand_name='extract')

    aggregator_parser = subparsers.add_parser(
        'aggregate', help='Aggregate fields')
    aggregator_parser.add_argument(
        '-f', '--field', help='The field to extract.',
        dest='fields', required=True, nargs='+')
    aggregator_parser.set_defaults(subcommand_name='aggregate')

    count_parser = subparsers.add_parser(
        'count', help='Count fields')
    count_parser.add_argument(
        '-f', '--field', help='The field to extract.',
        dest='fields', required=True, nargs='+')
    count_parser.set_defaults(subcommand_name='count')

    return parser
