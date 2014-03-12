import sys


from gdt.filters import (
    DirectionalFilter, MSISDNFilter,
    TimestampFilter, FilterPipeline)


class GDT(object):

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
