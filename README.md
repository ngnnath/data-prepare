# Data-prepare

Description : Convert  csv file to parquet file


###How to package it ?
```
python setup.py sdist
```
### Test it 
create  virtual env 
```
py setup.py install
```

Some csv example is in the resources directory

###Info
-   Scalability

a option is available to chunk the csv in small part and register the file parquet by a suffix.
valid_X.parquet and invalid_X.parquet

-    Versionning

I suppose is the schema of the csv, that why i add a option to take in parameter the schema file.
If the csv file is an export of a database (an export that can be daily), I choose to timestamp the file by 
naming it with a prefix.

Example: 

AAAAMMDD_HH_MM_SS_filename_[valid/invalid]_[partnumber].txt




- If we restart the program ?