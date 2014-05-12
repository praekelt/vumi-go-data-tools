import csv
import json


class CSVMessageCodec(object):

    def __init__(self, stdin, stdout, write_header=True):
        self.reader = csv.DictReader(stdin)
        self.writer = csv.DictWriter(
            stdout, fieldnames=self.reader.fieldnames)
        if write_header:
            # writer.writeheader() only available in py27
            self.writer.writerow(
                dict(zip(self.reader.fieldnames, self.reader.fieldnames)))

    def readrows(self):
        return self.reader

    def writerow(self, message):
        self.writer.writerow(message)


class JSONMessageCodec(object):

    def __init__(self, stdin, stdout, write_header=True):
        self.stdin = stdin
        self.stdout = stdout

    def readrows(self):
        for line in self.stdin:
            yield json.loads(line)

    def writerow(self, message):
        row = json.dumps(message)
        self.stdout.write('%s\n' % (row.encode('utf-8'),))
