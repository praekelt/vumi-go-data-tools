from StringIO import StringIO
from unittest import TestCase

from gdt.vumigomessage import DoAllTheThings


class GdtTestCase(TestCase):

    def setUp(self):
        pass

    def get_datt(self, options):
        defaults = {
            'foo': 'bar'
        }
        defaults.update(options)
        datt = DoAllTheThings(defaults)
        datt.stdin = StringIO()
        datt.stdout = StringIO()
        return datt

    def test_something(self):
        datt = self.get_datt({
            'bar': 'baz'
        })
        datt.readline('some,csv,data')
        self.assertEqual(
            datt.stdout.getValue(), 'foo')
