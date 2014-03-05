from StringIO import StringIO
from unittest import TestCase

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

    def test_date_match(self):
        datt = self.get_datt({
            'start': '2013-09-09 19:20',
            'end': '2013-09-09 19:40'
        })
        
        datt.stdin.write(datt.header)
        datt.stdin.write('2013-09-09 19:24:03.289543,+27817030792,*120*8864*1203#,None,af266289e40949388b5a8cacb4a2d13a,None,new,ussd,inbound,,,,default')
        datt.run()
        # OR run directly?
        # datt.filtered('2013-09-09 19:24:03.289543,+27817030792,*120*8864*1203#,None,af266289e40949388b5a8cacb4a2d13a,None,new,ussd,inbound,,,,default')
        self.assertEqual(
            datt.stdout.getvalue(), 'foo')
