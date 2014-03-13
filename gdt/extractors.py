import csv
import sys

from collections import defaultdict

import dateutil.parser

from gdt.codec import CSVMessageCodec


class ExtractorException(Exception):
    pass


class Extractor(object):

    field_names = []

    def __init__(self):
        self._chain = []

    def aggregate(self, row):
        raise NotImplemented('Subclasses should implement')

    def chain(self, filter):
        self._chain.append(filter)
        return self

    def get_field_names(self):
        return self.field_names

    def get_chained_field_names(self):
        return reduce(
            lambda acc, agg: acc.append(agg.get_field_names()),
            self._chain, self.get_field_names())

    def process(self, row):
        return reduce(
            lambda accumulator, aggregator: aggregator.aggregate(accumulator),
            self._chain, self.aggregate(row))


class FieldExtractor(Extractor):

    def __init__(self, fields, date_format=None):
        super(FieldExtractor, self).__init__()
        self.fields = fields
        self.date_format = date_format
        self.data = defaultdict(int)

    def get_field_names(self):
        return ['timestamp'] + self.fields

    def aggregate(self, row):
        if self.date_format is not None:
            date = dateutil.parser.parse(row['timestamp'])
            date_str = date.strftime(self.date_format)
        else:
            date_str = date.isoformat()

        result = {
            'timestamp': date_str
        }

        for field in self.fields:
            result[field] = row[field]

        return result


class ExtractorPipeline(object):

    # NOTE: this always outputs CSV

    input_codec = CSVMessageCodec

    def __init__(self, aggregators=None, codec_class=None):
        self.aggregators = ([] if aggregators is None else aggregators)
        self.codec_class = (self.input_codec if codec_class is None
                            else codec_class)

    def add(self, aggregator):
        self.aggregators.append(aggregator)

    def empty(self):
        return len(self.aggregators) == 0

    def get_aggregator_field_names(self):
        return reduce(
            lambda acc, agg: acc + agg.get_chained_field_names(),
            self.aggregators, [])

    def process(self, stdin=sys.stdin, stdout=sys.stdout):
        input_codec = self.codec_class(stdin, stdout)
        field_names = self.get_aggregator_field_names()
        output = csv.DictWriter(stdout, fieldnames=field_names)
        output.writerow(dict(zip(field_names, field_names)))
        for row in input_codec.readrows():
            for aggregator in self.aggregators:
                output.writerow(aggregator.process(row))
