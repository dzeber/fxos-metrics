Job scripts to run using telemetry-server. 

* **dump_all.py** 
    Dump all records matching the filter
* **dump_format_ftu.py** 
    Extract necessary information from each FTU record as a dict of cleansed 
    values. Reducer counts occurrences of records with identical summaries.
* **count_records.py**
    Simple job to count records. Mostly intended for testing.
* **dump_format_appusage.py**
    Extract necessary information from each AU record. Cleanse values and count
    occurrences.


filters
-------

Specifications for which FxOS records to include when running a mapreduce job. 
These are JSON files which follow a schema prescribed in `telemetry-server`.

