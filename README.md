fxos-metrics
============

Tools and scripts for computing metrics for Firefox OS. This includes both 
mapreduce jobs to run using `telemetry-server`, as well postprocessing that can
be done locally.

Some of these scripts can be run on an adhoc basis, and others will be scheduled
crons.


filters
-------

Specifications for which FxOS records to include when running a mapreduce job. 
These are JSON files which follow a schema prescribed in `telemetry-server`.


jobs
----

Mapreduce jobs to be run using `telemetry-server`. These are python scripts
containing `map` and `reduce` functions.


processing
----------

Python scripts that apply postprocessing to the output of the mapreduce jobs, 
for example packaging the output records as CSV rows.


shared
------

Common functions that are used across several mapreduce job and processing 
scripts. The main focus of these is formatting data values and organizing 
output. 


lookup
------

Lookup tables for converting coded fields (eg. country codes or mobile/ICC codes
to human-readable values), stored in JSON format. 

The file `ftu-fields.json` functions as a whitelist to determine which 
individual values get retained for display in the dashboard for various fields. 
The whitelists function either by full or prefix matching depending on the 
field. Values that are not matched by the whitelist get grouped as "Other" in 
the dashboard dropdowns.

