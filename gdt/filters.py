import sys

import dateutil.parser

from gdt.codec import CSVMessageCodec


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

    def __init__(self, addr_type, msisdn):
        super(MSISDNFilter, self).__init__()
        if addr_type not in ['to_addr', 'from_addr']:
            raise FilterException
        self.addr_type = addr_type
        self.msisdn = msisdn

    def apply(self, row):
        return row.get(self.addr_type) == self.msisdn


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


class SessionEventFilter(Filter):

    def __init__(self, event_type):
        super(SessionEventFilter, self).__init__()
        self.event_type = event_type

    def apply(self, row):
        # Have to str() here because CSV string's the `None` values.
        return str(row.get('session_event')) == str(self.event_type)


class FilterPipeline(object):

    default_codec = CSVMessageCodec

    def __init__(self, filters=None, codec_class=None):
        self.filters = ([] if filters is None else filters)
        self.codec_class = (self.default_codec if codec_class is None
                            else codec_class)
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
