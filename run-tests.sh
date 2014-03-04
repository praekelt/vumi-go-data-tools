#/bin/bash
eval python vumigomessage.py -i test/messages-export-good.csv -o results.txt
r1=$?
eval python vumigomessage.py -i test/messages-export-bad.csv -o results.txt
r2=$?
exit $(($r1 + $r2))
