from StringIO import StringIO
from unittest import TestCase
import csv

from gdt.vumigomessage import VumiGoMessageParser


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
