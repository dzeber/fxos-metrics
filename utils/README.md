Common functions used in both AWS jobs and postprocessing scripts. 
The main component of these is formatting functions for sanitizing the raw 
data values. 


lookup
------

Lookup tables for converting short codes (eg. country codes or mobile/ICC 
codes) to display values. 
The file `ftu-fields.json` functions as a whitelist to 
determine which individual values get retained for display in the dashboard 
for various fields. 
The whitelists function either by full or prefix matching depending on the 
field. Values that are not matched by the whitelist get grouped as "Other" in 
the dashboard dropdowns.

