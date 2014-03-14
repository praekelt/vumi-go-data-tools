from unittest import TestCase

from gdt.aggregators import UniquesAggregator, SimpleAggregator


class AggregatorTestCase(TestCase):

    def test_uniques_aggregator(self):
        a = UniquesAggregator(['foo', 'bar'])
        a.aggregate({
            'timestamp': '2014',
            'foo': '1',
            'bar': '1',
        })
        a.aggregate({
            'timestamp': '2014',
            'foo': '1',
            'bar': '2',
        })

        self.assertEqual(
            list(a.get_data()),
            [{'timestamp': '2014', 'foo': 1, 'bar': 2}])

    def test_simple_aggregator(self):
        a = SimpleAggregator(['foo', 'bar'])
        a.aggregate({
            'timestamp': '2014',
            'foo': '1',
            'bar': '1',
        })
        a.aggregate({
            'timestamp': '2014',
            'foo': '1',
            'bar': '1',
        })
        self.assertEqual(
            list(a.get_data()),
            [{'timestamp': '2014', 'foo': 2, 'bar': 2}])
