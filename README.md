# Data-prepare

Description : Convert  csv file to parquet file


### How to package it ?
```
python setup.py sdist
```
### Test it 
create  virtual env  with python version 3.7

**Créer un python env**

```
virtualenv --python=/usr/bin/python3.7 py_env37
```

**Activate**
```
source venv/py_env37/bin/activate
```
Install

```
py setup.py install
```

run the tests to see if the setup works
```
py setup.py test
```

run it by calling the module 

```
py -m converter.csvtoparquet -f product_catalog.csv 
```

### Info
-   Scalability

a option is available to chunk the csv in small part and register the file parquet by a suffix.
valid_X.parquet and invalid_X.parquet

-    Versionning

I suppose is the schema of the csv, that why i add a option to take in parameter the schema file.
If the csv file is an export of a database (an export that can be daily), I choose to timestamp the file by 
naming it with a prefix.

Example: 

AAAAMMDD_HH_MM_SS_filename_[valid/invalid]_[partnumber].txt


-   If we restart the program ?

A new file will be create with a different timestamp.

## Next ?

- The next step will be to have a program that can restart at a point or in case the program is interrupted for X reasons.

- Test other lib to benchmark the performance : 

https://mungingdata.com/python/writing-parquet-pandas-pyspark-koalas/