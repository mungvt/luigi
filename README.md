# Luigi Project Training

## Construct

* Folder dryrun: dry-run local target files and a log file

* Folder csv: output csv files after run tasks

* Folder sql: save queries running by MySql

* Folder logic: logics file

* Folder model: models file

* Folder config: config file

* Folder unittest: unittest file

* Folder luigi_wf: luigi file

## Running:

* Firstly: create folder config, in the folder create config.py file with contain:
```python
host = 'link:port'
db = 'XXX'
user = 'XXX'
pw = 'XXX'
DRYRUN = True
```

* Create database name `db` variable in file `config.py`

* Push `airlines.json` file in working directory

* Running `luigid`

* Run ` PYTHONPATH='./logic:./model:./luigi_wf:./config' luigi --module luigi_wf AllTasks`

* Unittest
    * Change DRYRUN = False in config.py 
    * `PYTHONPATH='./logic:./model:./luigi_wf:./config:./unittest' nose2 --plugin=nose2.plugins.layers test_logic -v`

View flow in `localhost:8082`
