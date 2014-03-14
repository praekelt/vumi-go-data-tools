from unittest import TestCase

from gdt.extractors import FieldExtractor


class ExtractorTestCase(TestCase):

    def test_field_extractor(self):
        f = FieldExtractor(['foo', 'bar'], '%Y')
        self.assertEqual(
            f.process({
                'foo': '1',
                'bar': '2',
                'qux': '3',
                'timestamp': '2014-01-01',
            }),
            {
                'foo': '1',
                'bar': '2',
                'timestamp': '2014',
            })

    def test_field_extractor_chaining(self):
        f = FieldExtractor(['foo', 'bar'], '%Y')
        f.chain(FieldExtractor(['foo']))
        self.assertEqual(
            f.process({
                'foo': '1',
                'bar': '2',
                'qux': '3',
                'timestamp': '2014-01-01',
            }),
            {
                'foo': '1',
                'timestamp': '2014',
            })
