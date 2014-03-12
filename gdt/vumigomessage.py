import argparse
import csv
import dateutil.parser
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
        self.start = dateutil.parser.parse(start)
        self.end = (dateutil.parser.parse(end) if end is not None else None)
        if self.end and self.end < self.start:
            raise FilterException(
                'End timestamp must come after start timestamp.')

    def apply(self, row):
        vumitimestamp = dateutil.parser.parse(row['timestamp'])
        if self.end is not None:
            return self.start <= vumitimestamp < self.end
        return self.start <= vumitimestamp


class FilterPipeline(object):

    def __init__(self, filters=None):
        self.filters = ([] if filters is None else filters)
        self._chain = []

    def process(self, stdin=sys.stdin, stdout=sys.stdout):
        reader = csv.DictReader(stdin)
        writer = csv.DictWriter(stdout, fieldnames=reader.fieldnames)
        # writer.writeheader() only available in py27
        writer.writerow(dict(zip(reader.fieldnames, reader.fieldnames)))
        for row in reader:
            for filter_ in self.filters:
                if filter_.process(row):
                    writer.writerow(row)
                    break


class VumiGoMessageParser(object):

    stdin = sys.stdin
    stdout = sys.stdout


    def __init__(self, args):
        self.args = args


    def run(self):
        filters = []
        if 'msisdn' in self.args:
            filters.append(MSISDNFilter('from_addr', self.args['msisdn']))
            filters.append(MSISDNFilter('to_addr', self.args['msisdn']))
        if 'direction' in self.args:
            if self.args['direction'] == "all":
                filters.append(DirectionalFilter('inbound'))
                filters.append(DirectionalFilter('outbound'))
            else:
                filters.append(DirectionalFilter(self.args['direction']))
        if 'start' in self.args:
            if 'end' not in self.args:
                self.args['end'] = None
            filters.append(TimestampFilter(self.args['start'], self.args['end']))

        fp = FilterPipeline(filters)
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
              '(as ISO timestamp, e.g. 2013-09-01 01:00:00)'), required=False)
    parser.add_argument(
        '-e', '--end',
        help=('Date time to extract to '
              '(as ISO timestamp, e.g. 2013-09-10 03:00:00)'), required=False)

    args = parser.parse_args()
    gdt = VumiGoMessageParser(vars(args))
    gdt.run()
