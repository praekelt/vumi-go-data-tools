import argparse
import csv
import os
import pprint
import dateutil.parser
import sys


class DoAllTheThings(object):

    stdin = sys.stdin
    stdout = sys.stdout

    # List positions
    timestamp = 0
    from_addr = 1
    to_addr = 2
    content = 3
    message_id = 4
    in_reply_to = 5
    session_event = 6
    transport_type = 7
    direction = 8
    network_handover_status = 9
    network_handover_reason = 10
    delivery_status = 11
    endpoint = 12

    def __init__(self, args):
        self.args = args
        self.run_whatever_mike_wrote()

    def filtered(self, output_writer, message):
        # check message and write out if it matches filter
        matches = False
        if self.args.msisdn:
            matches = False # reset because if defined, needs to match
            if self.args.direction == "all" and self.args.msisdn in [message[from_addr], message[to_addr]]:
                matches = True
            elif self.args.direction == "inbound" and self.args.msisdn == message[from_addr]:
                matches = True
            elif self.args.direction == "outbound" and self.args.msisdn == message[to_addr]:
                matches = True
        if self.args.start and self.args.end:
            matches = False # reset because if defined, needs to match
            vumitimestamp = dateutil.parser.parse(message[timestamp])
            start = dateutil.parser.parse(self.args.start)
            end = dateutil.parser.parse(self.args.end)
            if (vumitimestamp > start and vumitimestamp < end):
                matches = True
        # output
        if matches:
            output_writer.writerow(message)

    def extracted(self, message):
        # check message and collect metrics
        pass

    def run_whatever_mike_wrote(self):
        errors = []
        # FIXME: use self.stdin & self.stdout here
        #        I'd suggest you create a single readline() function
        #        here that reads whatever is in stdin, in your tests
        #        you'd call readline() directly to simulate input from
        #        stdin.
        with open(os.path.join(os.path.abspath(os.path.dirname(__file__)), self.args.input), 'rb') as inputfile:
            messages = csv.reader(inputfile, delimiter=',', quotechar='"')
            with open(os.path.join(os.path.abspath(os.path.dirname(__file__)), self.args.output), 'wb') as outputfile:
                output_writer = csv.writer(outputfile)
                data_started = False
                line = 0
                multiline = False
                multiline_buffer = []

                for message in messages:
                    line += 1
                    if message == ["timestamp", "from_addr", "to_addr", "content",
                            "message_id", "in_reply_to", "session_event", "transport_type",
                            "direction", "network_handover_status", "network_handover_reason",
                            "delivery_status", "endpoint"]:
                        # Ignore header row
                        data_started = True
                    elif data_started and len(message) == 13:
                        # Start processing
                        filtered(output_writer, message)
                        extracted(message)
                    elif len(message) != 13:
                        errors.append([message, line, "Strange fields in row"])
                    else:
                        errors.append([message, line, "Unparsable entry"])

        if len(errors) == 0:
            print 0
        else:
            pprint.pprint(errors)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Vumi Go Messages CSV Parser')
    parser.add_argument('-i','--input', help='Input CSV file name',required=True)
    parser.add_argument('-o','--output', help='Output file name', required=True)
    parser.add_argument('-m','--msisdn', help='MSISDN to extract messages', required=False)
    parser.add_argument('-d','--direction', help='Message direction to extract', required=False, choices=['inbound', 'outbound', 'all'])
    parser.add_argument('-s','--start', help='Date time to start from (as ISO timestamp, e.g. 2013-09-01 01:00:00)', required=False)
    parser.add_argument('-e','--end', help='Date time to extract to (as ISO timestamp, e.g. 2013-09-10 03:00:00)', required=False)

    args = parser.parse_args()
    DoAllTheThings(vars(args))
