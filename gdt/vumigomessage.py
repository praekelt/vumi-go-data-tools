import argparse
import csv
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

    errors = []

    header = ["timestamp", "from_addr", "to_addr", "content",
                "message_id", "in_reply_to", "session_event", "transport_type",
                "direction", "network_handover_status", "network_handover_reason",
                "delivery_status", "endpoint"]

    # Header positions
    timestamp = 0
    from_addr = 1
    to_addr = 2
    content = 3
    message_id = 4
    in_reply_to = 5
    session_event = 6
    transport_type = 7
    direction = 8
    network_handover_status = 9
    network_handover_reason = 10
    delivery_status = 11
    endpoint = 12

    def __init__(self, args):
        self.args = args

    def filtered(self, message):
        # check message and write out if it matches filter
        matches = False
        if 'msisdn' in self.args:
            matches = False # reset because if defined, needs to match
            if self.args['direction'] == "all" and self.args['msisdn'] in [message[self.from_addr], message[self.to_addr]]:
                matches = True
            elif self.args['direction'] == "inbound" and self.args['msisdn'] == message[self.from_addr]:
                matches = True
            elif self.args['direction'] == "outbound" and self.args['msisdn'] == message[self.to_addr]:
                matches = True
        if 'start' in self.args and 'end' in self.args:
            matches = False # reset because if defined, needs to match
            vumitimestamp = dateutil.parser.parse(message[self.timestamp])
            start = dateutil.parser.parse(self.args['start'])
            end = dateutil.parser.parse(self.args['end'])
            if (vumitimestamp > start and vumitimestamp < end):
                matches = True
        # output
        if matches:
            csv.writer(self.stdout).writerow(message)

    def extracted(self, message):
        # check message and collect metrics
        pass

    def run(self):
        line = 0
        for message in csv.reader(self.stdin, delimiter=',', quotechar='"'):
            line += 1
            self.handle_message(message)
        if len(self.errors) != 0:
            self.stdout.write(unicode(self.errors))

    def handle_message(self, message):
        if message == self.header:
            pass
        elif len(message) == 13:
            # Start processing
            self.filtered(message)
            self.extracted(message)
        else:
            self.errors.append([message, "Unparsable entry"])

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
