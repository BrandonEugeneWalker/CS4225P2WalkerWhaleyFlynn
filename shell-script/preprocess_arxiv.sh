#!/bin/bash

input_file=$1
output_file=$2
grep -n "summary" $input_file | sed -e 's/^/Abstract/' | cut -f1,3- -d: > $output_file
