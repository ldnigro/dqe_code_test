# dqe_code_test
Data Quality Engineer Code Test

App
---
* pipeline.py main program
        
        Usage: python pipeline.py [config file]

* zomato.config     - App configuration file

* pipeline.log      - App log

Support:
-------
* config.py         - manage app configuration

* data_base.py      - read and write app state data into a SqlLite Database
                    - contains spark management

Modules
-------

* input.py          - read input data files

* file_check.py     - verify file constraints

* quality_check.py  - perform data quality validations and transformations

* output.py         - write output data and metadata

Output
------
Processed files / output.7z
