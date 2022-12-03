#! /bin/bash
mkdir Data
filename="Buckets.txt"
while read line;
do
	val=( $line )
	start_year=${val[0]}
	end_year=${val[1]/$'\r'/}
	newfile="./Data/${start_year}-${end_year}.txt"
	touch "${newfile}"
	while IFS="," read -r col1 col2 col3 col4 col5 col6
	do
		if [[ $col2 -ge $start_year ]] && [[ $col2 -lt $end_year ]];
		then
			echo "$col1, $col5" >> $newfile
		fi
	done < players.csv
	cat $newfile
done < $filename;

