import argparse
import dateutil.parser


def get_parser():

    parser = argparse.ArgumentParser(description='Vumi Go Data Tools')
    parser.add_argument(
        '-m', '--msisdn', help='MSISDN to extract messages', required=False)
    parser.add_argument(
        '-d', '--direction', help='Message direction to extract',
        required=False, choices=['inbound', 'outbound', 'all'])
    parser.add_argument(
        '-s', '--start',
        help=('Date time to start from '
              '(as ISO timestamp, e.g. 2013-09-01 01:00:00)'),
        required=False, type=dateutil.parser.parse)
    parser.add_argument(
        '-e', '--end',
        help=('Date time to extract to '
              '(as ISO timestamp, e.g. 2013-09-10 03:00:00)'),
        required=False, type=dateutil.parser.parse)

    return parser
