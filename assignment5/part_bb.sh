#! /bin/bash
while IFS="," read -r col1 col2 col3 col4 col5 col6
do
	if [[ $col6 == *","* ]];
	then
		echo "$col1, $col2, $col3, $col4, $col5, $col6"
	fi
done < players.csv

