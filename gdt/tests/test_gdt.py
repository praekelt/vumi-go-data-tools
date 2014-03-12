import csv
import json
from StringIO import StringIO
from unittest import TestCase
from datetime import datetime

from gdt.vumigomessage import (
    VumiGoMessageParser, DirectionalFilter, MSISDNFilter, TimestampFilter,
    FilterPipeline, FilterException, IsAReplyFilter, IsNotAReplyFilter,
    CSVMessageCodec, JSONMessageCodec)


class GdtTestCase(TestCase):

    HEADER = ("timestamp,from_addr,to_addr,content,message_id,in_reply_to,"
              "session_event,transport_type,direction,"
              "network_handover_status,network_handover_reason,"
              "delivery_status,endpoint\r\n")
    INBOUND = ("2013-09-10 19:24:03.289543,+27817030792,*120*8864*1203#,"
               ",af266289e40949388b5a8cacb4a2d13a,,new,ussd,inbound"
               ",,,,default\r\n")
    OUTBOUND = ("2013-09-11 19:24:03.289543,*120*8864*1203#,+27817030792,"
                ",af266289e40949388b5a8cacb4a2d13b,,resume,ussd,outbound"
                ",,,,default\r\n")
    INBOUND_2 = ("2013-09-09 19:24:03.289543,+27817030710,*120*8864*1203#,"
               ",af266289e40949388b5a8cacb4a2d13a,,new,ussd,inbound"
               ",,,,default\r\n")
    OUTBOUND_2 = ("2013-09-09 19:24:03.289543,*120*8864*1203#,+27817030710,"
                ",af266289e40949388b5a8cacb4a2d13b,,resume,ussd,outbound"
                ",,,,default\r\n")

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
            'start': datetime(2013, 9, 10, 19, 20),
            'end': datetime(2013, 9, 12, 19, 40)
        })
        SAMPLE = self.HEADER + self.INBOUND + self.OUTBOUND
        output = self.parse(parser, SAMPLE)
        self.assertEqual(output, SAMPLE)

    def test_date_not_match(self):
        parser = self.get_parser({
            'start': datetime(2013, 9, 10, 19, 20),
            'end': datetime(2013, 9, 12, 19, 40)
        })
        SAMPLE = self.HEADER + self.INBOUND_2 + self.OUTBOUND_2
        output = self.parse(parser, SAMPLE)
        self.assertEqual(output, self.HEADER)

    def test_date_msisdn_match_inbound(self):
        parser = self.get_parser({
            'msisdn': '+27817030710',
            'direction': 'inbound'
        })
        SAMPLE = self.HEADER + self.INBOUND_2 + self.OUTBOUND_2
        output = self.parse(parser, SAMPLE)
        self.assertEqual(output, self.HEADER + self.INBOUND_2)

    def test_date_msisdn_match_outbound(self):
        parser = self.get_parser({
            'msisdn': '+27817030710',
            'direction': 'outbound'
        })
        SAMPLE = self.HEADER + self.INBOUND_2 + self.OUTBOUND_2
        output = self.parse(parser, SAMPLE)
        self.assertEqual(output, self.HEADER + self.OUTBOUND_2)

    def test_date_msisdn_match_all(self):
        parser = self.get_parser({
            'msisdn': '+27817030710',
            'direction': 'all'
        })
        SAMPLE = self.HEADER + self.INBOUND_2 + self.OUTBOUND_2
        output = self.parse(parser, SAMPLE)
        self.assertEqual(output, SAMPLE)

    def test_date_msisdn_no_direction(self):
        parser = self.get_parser({
            'msisdn': '+27817030710'
        })
        SAMPLE = self.HEADER + self.INBOUND + self.OUTBOUND + self.INBOUND_2 + self.OUTBOUND_2
        output = self.parse(parser, SAMPLE)
        self.assertEqual(output, self.HEADER + self.INBOUND_2 + self.OUTBOUND_2)

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
        f = TimestampFilter(datetime(2013, 1, 1))
        self.assertTrue(f.apply({'timestamp': '2013-01-01'}))
        self.assertFalse(f.apply({'timestamp': '2012-01-01'}))

        f = TimestampFilter(datetime(2013, 1, 1), datetime(2013, 2, 1))
        self.assertTrue(f.apply({'timestamp': '2013-01-01'}))
        self.assertFalse(f.apply({'timestamp': '2013-03-01'}))

        self.assertRaises(
            FilterException, TimestampFilter,
            datetime(2013, 1, 1), datetime(2000, 1, 1))

    def test_filter_chaining(self):
        f = TimestampFilter(datetime(2013, 1, 1)).chain(
            DirectionalFilter('inbound'))
        self.assertTrue(f.process({
            'direction': 'inbound', 'timestamp': '2013-01-01'}))
        self.assertFalse(f.process({
            'direction': 'outbound', 'timestamp': '2013-01-01'}))
        self.assertFalse(f.process({
            'direction': 'inbound', 'timestamp': '2000-01-01'}))

        f = TimestampFilter(datetime(2013, 1, 1),
                            datetime(2013, 12, 31)).chain(
                                DirectionalFilter('inbound'))

        self.assertTrue(f.process({
            'direction': 'inbound', 'timestamp': '2013-06-01'}))
        self.assertFalse(f.process({
            'direction': 'inbound', 'timestamp': '2014-06-01'}))


class CSVFilterPipelineTestCase(TestCase):

    CODEC_CLASS = CSVMessageCodec

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
        fp = FilterPipeline([DirectionalFilter('inbound')],
                            codec=self.CODEC_CLASS)
        stdin = StringIO(self.SAMPLE)
        stdout = StringIO()
        fp.process(stdin=stdin, stdout=stdout)
        self.assertLineEqual(stdout.getvalue(), self.HEADER + self.INBOUND)

    def test_filter_pipeline_chaining(self):
        fp = FilterPipeline([
            DirectionalFilter('inbound').chain(
                MSISDNFilter('from_addr', '+27817030792')),
            TimestampFilter('2013-09-10 00:00:00', '2013-09-10 23:59:59')
        ], codec=self.CODEC_CLASS)
        stdin = StringIO(self.SAMPLE)
        stdout = StringIO()
        fp.process(stdin=stdin, stdout=stdout)
        self.assertLineEqual(stdout.getvalue(), self.HEADER + self.INBOUND)

    def assertLineEqual(self, line1, line2):
        self.assertEqual(line1, line2)


class JSONFilterPipelineTestCase(TestCase):

    CODEC_CLASS = JSONMessageCodec

    HEADER = ''
    INBOUND = json.dumps({
        "transport_name": "mtn_nigeria_ussd_transport",
        "transport_metadata": {},
        "group": None,
        "from_addr": "+27817030792",
        "timestamp": "2013-09-10 19:24:03.289543",
        "provider": "mtn_nigeria",
        "to_addr": "*120*8864*1203#",
        "content": "",
        "routing_metadata": {
            "go_hops": [],
            "endpoint_name": "default"
        },
        "message_version": "20110921",
        "transport_type": "ussd",
        "in_reply_to": None,
        "session_event": "new",
        "message_id": "af266289e40949388b5a8cacb4a2d13a",
        "message_type": "user_message",
    })

    OUTBOUND = json.dumps({
        "transport_name": "mtn_nigeria_ussd_transport",
        "transport_metadata": {},
        "group": None,
        "from_addr": "*120*8864*1203#",
        "timestamp": "2013-09-11 19:24:03.289543",
        "provider": "mtn_nigeria",
        "to_addr": "27123456789",
        "content": "",
        "routing_metadata": {
            "go_hops": [],
            "endpoint_name": "default"
        },
        "message_version": "20110921",
        "transport_type": "ussd",
        "in_reply_to": "af266289e40949388b5a8cacb4a2d13a",
        "session_event": "resume",
        "message_id": "af266289e40949388b5a8cacb4a2d13b",
        "message_type": "user_message",
    })

    SAMPLE = '\n'.join([INBOUND, OUTBOUND])

    def test_filter_pipeline(self):
        fp = FilterPipeline([IsNotAReplyFilter()],
                            codec=self.CODEC_CLASS)
        stdin = StringIO(self.SAMPLE)
        stdout = StringIO()
        fp.process(stdin=stdin, stdout=stdout)
        self.assertLineEqual(stdout.getvalue(), self.HEADER + self.INBOUND)

    def test_filter_pipeline_chaining(self):
        fp = FilterPipeline([
            IsAReplyFilter().chain(
                MSISDNFilter('from_addr', '+27817030792')),
            TimestampFilter('2013-09-10 00:00:00', '2013-09-10 23:59:59')
        ], codec=self.CODEC_CLASS)
        stdin = StringIO(self.SAMPLE)
        stdout = StringIO()
        fp.process(stdin=stdin, stdout=stdout)
        self.assertLineEqual(stdout.getvalue(), self.HEADER + self.INBOUND)

    def assertLineEqual(self, line1, line2):
        d1 = json.loads(line1)
        d2 = json.loads(line2)
        self.assertEqual(
            d1, d2,
            'Line 1:\n%s\n\nDoes not match Line 2:\n%s' % (
                json.dumps(d1, indent=2), json.dumps(d2, indent=2)))
