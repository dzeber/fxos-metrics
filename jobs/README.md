Job scripts to run using telemetry-server. Python files contain the map and reduce phases, and shell scripts are launchers.

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

