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

## Usage

::

  $ gdt --help

## Examples

::

  $ cat gdt/tests/messages-export-good.csv | gdt msisdn -m +27817030792 -t to_addr | gdt direction -d outbound | gdt daterange -s "2013-09-09 19:24" -e "2013-09-09 19:38"

  $ cat gdt/tests/messages-export-week-spread.csv | gdt weekrange -y 2013 -w 1 2 3 4

  $ cat gdt/tests/messages-export-good.csv | gdt session -t new

  $ cat gdt/tests/messages-export-good.csv | gdt contacts -a +123456 +123457

  $ cat gdt/tests/messages-export-good.csv | gdt contacts -a @contact_file.txt

  $ cat gdt/tests/messages-export-good.csv | gdt regex -f content -p "^we think" -i

  $ cat gdt/tests/messages-export-good.csv | gdt extract -f to_addr session_event -df "%M" 

  $ cat gdt/tests/messages-export-good.csv | gdt extract -f to_addr session_event -df "%M" | gdt aggregate -f to_addr

  $ cat gdt/tests/messages-export-good.csv | gdt extract -f to_addr session_event -df "%M" | gdt count -f to_addr

  $ cat gdt/tests/messages-export-week-spread.csv | gdt weekrange -y 2013 -w 1 2 3 4
