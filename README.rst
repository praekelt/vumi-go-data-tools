# vumi-go-data-tools

Praekelt Foundation does a lot of work using the Vumi Go messaging platform.
Vumi Go makes all data captured available as CSV files but as of yet does not
provide tools for doing pivot table based analysis on that data. The idea is
to develop set of reusable tools that allows a developer to quickly do pivot
tables on large CSV files that Vumi Go produces on behalf of the project owner
for post campaign analysis. These tools should give insight into session counts,
returning visitors and effectiveness of different approaches taken with differing
Vumi Go contact groups that interact with a service.

## Set up

::

  $ virtualenv ve
  $ . ve/bin/activate
  $ pip install -e .
  $ gdt --help

## Usage

```
usage: python -m gdt.vumigomessage -m [-h] [-m MSISDN]
                        [-d {inbound,outbound,all}] [-s START] [-e END]

optional arguments:
  -h, --help            show this help message and exit
  -m MSISDN, --msisdn MSISDN
                        MSISDN to extract messages
  -d {inbound,outbound,all}, --direction {inbound,outbound,all}
                        Message direction to extract
  -s START, --start START
                        Date time to start from (as ISO timestamp, e.g.
                        2013-09-01 01:00:00)
  -e END, --end END     Date time to extract to (as ISO timestamp, e.g.
                        2013-09-10 03:00:00)

```

## Examples

`less gdt/tests/messages-export-good.csv | python -m gdt.vumigomessage -m +27817030792 -d outbound -s "2013-09-09 19:24" -e "2013-09-09 19:38" > results.txt`

