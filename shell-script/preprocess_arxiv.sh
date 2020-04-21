#!/bin/bash
# Script for processing the arxiv data json file.
# Will find all the summaries in the json file and output them with line numbers to the given output file.
# Authors: Luke Whaley, Brandon Walker, Kevin Flynn

# The number of arguments passed into the script.
arg_count=$#

# Checks if a valid number of arguments was given, if not it displays an error and the usage statement.
if [ $arg_count -ne 3 ]
	then
		echo "Invalid number of arguments provided."
		echo "Usage: ./preprocess.sh <input> <processed output> <count output>"
else
        #Reading the command line arguments and storing them as variables.
		input_file=$1
		output_file=$2
		count_file=$3
		
		echo "Processing $input_file and outputting results to $output_file"

        # grep is a utility for searching files for a given token
        # -n in grep denotes displaying the matched line with its line number
        # sed is a stream editor that allows us to adapt the contents of the file the way we want it.
        # -e in sed denotes using a script, 
        # with 's/^/Abstract' getting the line number from the grep output and combining it with 'Abstract<number>: ' and then the contents of the line.
		grep -n "summary" $input_file | sed -e 's/^/Abstract/' | cut -f1,3- -d: > $output_file
		
		echo "Counting the number of lines in $output_file and writing it to $count_file, as well as writing the first and last line to the file."
		
		# wc, or wordcount is a program that can be used to count words, characters, and the number of lines in a file.
		# -l denotes counting lines, while >> appends the results to the given file.
		wc -l $output_file >> $count_file
		
		# head is used to read the top 10 lines of a file by default, but with the argument -1 it only reads the first line of a file.
		# >> has the results of the command appended to the given file
		head -1 $output_file >> $count_file
		
		# tail is used to read the bottom 10 lines of a file by default, but with the argument -1 it only reads the last line of a file.
		# >> has the results of the command appended to the given file.
		tail -1 $output_file >> $count_file
		echo "Job complete, view the results in $output_file and $count_file."
fi