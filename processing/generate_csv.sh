#!/bin/bash

## Download FTU records, sanitize and output as CSV.

## Download FxOS data scripts. 
git clone https://github.com/dzeber/fxos-metrics.git
chmod u+x fxos-metrics/runjob.sh

output=ftu_data.csv
job=dump_csv.py

fxos-metrics/runjob.sh "$job" "$output"

