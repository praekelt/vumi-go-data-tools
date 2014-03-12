import argparse
import dateutil.parser


def get_parser():

    parser = argparse.ArgumentParser(description='Vumi Go Data Tools')

    subparsers = parser.add_subparsers(help='use `command --help`.')

    msisdn_parser = subparsers.add_parser(
        'msisdn', help='Filter on an msisdn')
    msisdn_parser.add_argument(
        '-m', '--msisdn', help='MSISDN to extract messages for',
        required=True)
    msisdn_parser.add_argument(
        '-t', '--addr-type',
        help='Which address type to filter on.',
        choices=['to_addr', 'from_addr'], required=True)

    daterange_parser = subparsers.add_parser(
        'daterange', help='Filter on a date range.')
    daterange_parser.add_argument(
        '-s', '--start',
        help=('Date time to start from '
              '(as ISO timestamp, e.g. 2013-09-01 01:00:00)'),
        required=False, type=dateutil.parser.parse)
    daterange_parser.add_argument(
        '-e', '--end',
        help=('Date time to extract to '
              '(as ISO timestamp, e.g. 2013-09-10 03:00:00)'),
        required=False, type=dateutil.parser.parse)

    direction_parser = subparsers.add_parser(
        'direction', help='Filter on message direction.')
    direction_parser.add_argument(
        '-d', '--direction', help='Message direction to extract',
        required=False, choices=['inbound', 'outbound'])

    return parser
