import argparse
import csv
from datetime import datetime
import dateutil.parser
import json
import sys


class FilterException(Exception):
    pass


class Filter(object):

    def __init__(self):
        self._chain = []

    def apply(self, row):
        raise NotImplemented('Subclasses should implement')

    def chain(self, filter):
        self._chain.append(filter)
        return self

    def process(self, row):
        return reduce(
            lambda accumulator, filter_: (
                (accumulator and filter_.apply(row))
                if accumulator else accumulator),
            self._chain, self.apply(row))


class DirectionalFilter(Filter):

    def __init__(self, direction):
        super(DirectionalFilter, self).__init__()
        self.direction = direction

    def apply(self, row):
        return row.get('direction') == self.direction


class IsAReplyFilter(Filter):

    def apply(self, row):
        return row.get('in_reply_to')


class IsNotAReplyFilter(IsAReplyFilter):

    def apply(self, row):
        return not super(IsNotAReplyFilter, self).apply(row)


class MSISDNFilter(Filter):

    def __init__(self, addr_direction, msisdn):
        super(MSISDNFilter, self).__init__()
        if addr_direction not in ['to_addr', 'from_addr']:
            raise FilterException
        self.addr_direction = addr_direction
        self.msisdn = msisdn

    def apply(self, row):
        return row.get(self.addr_direction) == self.msisdn


class TimestampFilter(Filter):

    def __init__(self, start, end=None):
        super(TimestampFilter, self).__init__()
        self.start = start
        self.end = end
        if self.end and self.end < self.start:
            raise FilterException(
                'End timestamp must come after start timestamp.')

    def apply(self, row):
        vumitimestamp = dateutil.parser.parse(row['timestamp'])
        if self.end is not None:
            return self.start <= vumitimestamp < self.end
        return self.start <= vumitimestamp


class CSVMessageCodec(object):

    def __init__(self, stdin, stdout):
        self.reader = csv.DictReader(stdin)
        self.writer = csv.DictWriter(
            stdout, fieldnames=self.reader.fieldnames)
        # writer.writeheader() only available in py27
        self.writer.writerow(
            dict(zip(self.reader.fieldnames, self.reader.fieldnames)))

    def readrows(self):
        return self.reader

    def writerow(self, message):
        self.writer.writerow(message)


class JSONMessageCodec(object):

    def __init__(self, stdin, stdout):
        self.stdin = stdin
        self.stdout = stdout

    def readrows(self):
        for line in self.stdin:
            yield json.loads(line)

    def writerow(self, message):
        json.dump(message, fp=self.stdout)


class FilterPipeline(object):

    default_codec = CSVMessageCodec

    def __init__(self, filters=None, codec=None):
        self.filters = ([] if filters is None else filters)
        self.codec_class = (self.default_codec if codec is None else codec)
        self._chain = []

    def add(self, filter):
        self.filters.append(filter)

    def empty(self):
        return len(self.filters) == 0

    def process(self, stdin=sys.stdin, stdout=sys.stdout):
        codec = self.codec_class(stdin, stdout)
        for row in codec.readrows():
            for filter_ in self.filters:
                if filter_.process(row):
                    codec.writerow(row)
                    break


class VumiGoMessageParser(object):

    stdin = sys.stdin
    stdout = sys.stdout

    def __init__(self, args):
        self.args = args

    def run(self):
        fp = FilterPipeline()
        for arg in self.args:
            if arg == 'msisdn':
                if 'direction' in self.args:  # do we need to chain?
                    if self.args['direction'] == "all":
                        fp.add(DirectionalFilter('inbound').chain(
                            MSISDNFilter('from_addr', self.args['msisdn'])))
                        fp.add(DirectionalFilter('outbound').chain(
                            MSISDNFilter('to_addr', self.args['msisdn'])))
                    elif self.args['direction'] == "inbound":
                        fp.add(DirectionalFilter('inbound').chain(
                            MSISDNFilter('from_addr', self.args['msisdn'])))
                    elif self.args['direction'] == "outbound":
                        fp.add(DirectionalFilter('outbound').chain(
                            MSISDNFilter('to_addr', self.args['msisdn'])))
                else:  # no chain required
                    fp.add(MSISDNFilter('to_addr', self.args['msisdn']))
                    fp.add(MSISDNFilter('from_addr', self.args['msisdn']))

            if arg == 'direction' and 'msisdn' not in self.args:
                if self.args['direction'] == "all":
                    fp.add(DirectionalFilter('inbound'))
                    fp.add(DirectionalFilter('outbound'))
                else:
                    fp.add(DirectionalFilter(self.args['direction']))

            if arg == 'start':
                if 'end' not in self.args:
                    self.args['end'] = None
                if fp.empty():
                    fp.add(TimestampFilter(self.args['start'],
                                           self.args['end']))
                else:
                    for link in fp.filters:
                        link.chain(TimestampFilter(self.args['start'],
                                                   self.args['end']))

        fp.process(stdin=self.stdin, stdout=self.stdout)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Vumi Go Messages CSV Parser')
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

    args = parser.parse_args()
    gdt = VumiGoMessageParser(vars(args))
    gdt.run()
