#! /bin/bash

filename="/media/mangy007/Data\ Drive/Backup/Personal/MTech/pg_software_lab/assignment5/part_3b_output.txt"
touch $filename

timestamp=$(date +%T)

echo ps aux --sort -%cpu | head -2 | tail -1 | awk "{print $1}" >> $filename
cat $filename