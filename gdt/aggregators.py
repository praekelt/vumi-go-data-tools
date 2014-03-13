import csv
import sys
from collections import defaultdict

from gdt.codec import CSVMessageCodec


class Aggregator(object):

    field_names = []

    def aggregate(self, row):
        raise NotImplemented('Subclasses should implement')

    def get_field_names(self):
        return self.field_names

    def get_data(self):
        raise NotImplemented('To be implemented by subclass.')


class UniquesAggregator(Aggregator):

    def __init__(self, fields):
        super(UniquesAggregator, self).__init__()
        self.fields = fields
        self.data = defaultdict(lambda: defaultdict(set))

    def get_field_names(self):
        return ['timestamp'] + self.fields

    def aggregate(self, row):
        d = self.data[row['timestamp']]
        for field in self.fields:
            d[field].update([row[field]])
        return d

    def get_data(self):
        for timestamp in sorted(self.data.keys()):
            d = {
                'timestamp': timestamp,
            }
            for field, uniques in self.data[timestamp].items():
                d[field] = len(uniques)

            yield d


class AggregatorPipeline(object):

    # NOTE: this always outputs CSV

    input_codec = CSVMessageCodec

    def __init__(self, aggregator, codec_class=None):
        self.aggregator = aggregator
        self.codec_class = (self.input_codec if codec_class is None
                            else codec_class)

    def process(self, stdin=sys.stdin, stdout=sys.stdout):
        input_codec = self.codec_class(stdin, stdout, write_header=False)
        field_names = self.aggregator.get_field_names()
        output = csv.DictWriter(stdout, fieldnames=field_names)
        output.writerow(dict(zip(field_names, field_names)))
        for row in input_codec.readrows():
            self.aggregator.aggregate(row)

        for result in self.aggregator.get_data():
            output.writerow(result)
