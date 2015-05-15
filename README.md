fxos-metrics
============

Tools and scripts for computing metrics for Firefox OS. This includes both 
mapreduce jobs to run using `telemetry-server`, as well postprocessing that can
be done locally.

Some of these scripts can be run on an adhoc basis, and others will be scheduled
crons.


awsjobs
-------

Mapreduce jobs to be run using `telemetry-server`. These are python scripts
containing `map` and `reduce` functions.


postprocessing
----------

Scripts to package extracted raw data into CSVs for powering dashboards and 
adhoc analysis.


utils
-----

Common functions used in both AWS jobs and postprocessing scripts. 

The main component of these is formatting functions for sanitizing the raw 
data values. 

