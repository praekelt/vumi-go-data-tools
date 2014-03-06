from StringIO import StringIO
from unittest import TestCase
import csv

from gdt.vumigomessage import (
    VumiGoMessageParser, DirectionalFilter, MSISDNFilter, TimestampFilter,
    FilterPipeline, FilterException)


class GdtTestCase(TestCase):

    def setUp(self):
        pass

    def get_datt(self, options):
        defaults = {
        }
        defaults.update(options)
        datt = VumiGoMessageParser(defaults)
        datt.stdin = StringIO()
        datt.stdout = StringIO()
        datt.header = "timestamp,from_addr,to_addr,content,message_id,in_reply_to,session_event,transport_type,direction,network_handover_status,network_handover_reason,delivery_status,endpoint"
        return datt

    def load_and_read(self, datt, line):
        datt.stdin.write(line)
        datt.stdin.seek(0)
        reader = csv.reader(datt.stdin)
        return reader.next()

    def test_date_match(self):
        datt = self.get_datt({
            'start': '2013-09-09 19:20',
            'end': '2013-09-09 19:40'
        })
        line = "2013-09-09 19:24:03.289543,+27817030792,*120*8864*1203#,,af266289e40949388b5a8cacb4a2d13a,,new,ussd,inbound,,,,default\r\n"
        datt.handle_message(self.load_and_read(datt, line))
        self.assertEqual(
            datt.stdout.getvalue(), line)

    def test_date_not_match(self):
        datt = self.get_datt({
            'start': '2013-09-09 19:20',
            'end': '2013-09-09 19:40'
        })
        line = "2013-09-10 19:24:03.289543,+27817030792,*120*8864*1203#,,af266289e40949388b5a8cacb4a2d13a,,new,ussd,inbound,,,,default\r\n"
        datt.handle_message(self.load_and_read(datt, line))
        self.assertEqual(
            datt.stdout.getvalue(), '')


class RefactoredGdtTestCase(TestCase):

    def get_parser(self, options):
        defaults = {
            # some parser argparse defaults here
        }
        defaults.update(options)
        return VumiGoMessageParser(defaults)

    def parse(self, parser, csv):
        parser.stdin = StringIO(csv)
        parser.stdout = StringIO()
        parser.run()
        return parser.stdout.getvalue()

    def test_date_match(self):
        parser = self.get_parser({
            'start': '2013-09-09 19:20',
            'end': '2013-09-09 19:40'
        })
        line = ("2013-09-09 19:24:03.289543,+27817030792,*120*8864*1203#,"
                ",af266289e40949388b5a8cacb4a2d13a,,new,ussd,inbound"
                ",,,,default\r\n")
        output = self.parse(parser, line)
        self.assertEqual(output, line)

    def test_date_not_match(self):
        parser = self.get_parser({
            'start': '2013-09-09 19:20',
            'end': '2013-09-09 19:40'
        })
        line = ("2013-09-10 19:24:03.289543,+27817030792,*120*8864*1203#,"
                ",af266289e40949388b5a8cacb4a2d13a,,new,ussd,inbound"
                ",,,,default\r\n")
        output = self.parse(parser, line)
        self.assertEqual(output, '')


class FilterTestCase(TestCase):

    def test_directional_filter(self):
        f = DirectionalFilter('inbound')
        self.assertTrue(f.apply({'direction': 'inbound'}))
        self.assertFalse(f.apply({'direction': 'outbound'}))
        self.assertFalse(f.apply({}))

    def test_msisdn_filter(self):
        f = MSISDNFilter('to_addr', '123')
        self.assertTrue(f.apply({'to_addr': '123'}))
        self.assertFalse(f.apply({'from_addr': '123'}))

        f = MSISDNFilter('from_addr', '123')
        self.assertTrue(f.apply({'from_addr': '123'}))
        self.assertFalse(f.apply({'to_addr': '123'}))

    def test_timestamp_filter(self):
        f = TimestampFilter('2013-01-01')
        self.assertTrue(f.apply({'timestamp': '2013-01-01'}))
        self.assertFalse(f.apply({'timestamp': '2012-01-01'}))

        f = TimestampFilter('2013-01-01', '2013-02-01')
        self.assertTrue(f.apply({'timestamp': '2013-01-01'}))
        self.assertFalse(f.apply({'timestamp': '2013-03-01'}))

        self.assertRaises(
            FilterException, TimestampFilter,
            '2013-01-01', '2000-01-01')

    def test_filter_chaining(self):
        f = TimestampFilter('2013-01-01').chain(
            DirectionalFilter('inbound'))
        self.assertTrue(f.process({
            'direction': 'inbound', 'timestamp': '2013-01-01'}))
        self.assertFalse(f.process({
            'direction': 'outbound', 'timestamp': '2013-01-01'}))
        self.assertFalse(f.process({
            'direction': 'inbound', 'timestamp': '2000-01-01'}))

        f = TimestampFilter('2013-01-01', '2013-12-31').chain(
            DirectionalFilter('inbound'))

        self.assertTrue(f.process({
            'direction': 'inbound', 'timestamp': '2013-06-01'}))
        self.assertFalse(f.process({
            'direction': 'inbound', 'timestamp': '2014-06-01'}))


class FilterPipelineTestCase(TestCase):

    HEADER = ("timestamp,from_addr,to_addr,content,message_id,in_reply_to,"
              "session_event,transport_type,direction,"
              "network_handover_status,network_handover_reason,"
              "delivery_status,endpoint\r\n")
    INBOUND = ("2013-09-10 19:24:03.289543,+27817030792,*120*8864*1203#,"
               ",af266289e40949388b5a8cacb4a2d13a,,new,ussd,inbound"
               ",,,,default\r\n")
    OUTBOUND = ("2013-09-11 19:24:03.289543,+27817030792,27123456789,"
                ",af266289e40949388b5a8cacb4a2d13b,,resume,ussd,outbound"
                ",,,,default\r\n")
    SAMPLE = HEADER + INBOUND + OUTBOUND

    def test_filter_pipeline(self):
        fp = FilterPipeline([DirectionalFilter('inbound')])
        stdin = StringIO(self.SAMPLE)
        stdout = StringIO()
        fp.process(stdin=stdin, stdout=stdout)
        self.assertEqual(stdout.getvalue(), self.HEADER + self.INBOUND)

    def test_filter_pipeline_chaining(self):
        fp = FilterPipeline([
            DirectionalFilter('inbound').chain(
                MSISDNFilter('from_addr', '+27817030792')),
            TimestampFilter('2013-09-10 00:00:00', '2013-09-10 23:59:59')
        ])
        stdin = StringIO(self.SAMPLE)
        stdout = StringIO()
        fp.process(stdin=stdin, stdout=stdout)
        self.assertEqual(stdout.getvalue(), self.HEADER + self.INBOUND)
