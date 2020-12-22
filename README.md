I would like to share one of the fastest & automated way of ingesting different versions of application tables dump to database, we achieved this using generators to lazy load, concurrent.futures.ProcessPoolExecutor to load data in parallel and PostgreSQL inheritence concept to maintain different versions of the application.

For this demo, I have two ZIP files which are a dump of same application but different database versions.
ZIP File v1 has 3 JSON files and 3 data files. The same goes with v2.
JSON files represent schema of the table and DAT file are a kind of flat file

## What changes from v1 to v2?

- Application features change time over time which also changes the schema of the underlying database, however, while deploying teams choose to deploy the changes region by region i.e. home countries/region experiences the changes earlier than other regions.
- Problems arise when you're analyzing/dashboarding complete aggregated data of all regions onto a single pane.
- To resolve this, we chose PostgreSQL Inheritence concept to maintain all versions of the applications in one single DB and works for dashboards with the common fields available in all versions.

## Working Principle:

- ProcessPoolExecutor to churn up processes and scan for JSON files in each dump file 
- Prepare a catalogue of files across all compressed files to be ingested
- ProcessPoolExecutor uses processes from the pool to loop through the catalogue and ingestion data
- zipfile.ZipFile class allows to access and read files inside a compressed file
- Python iterator to chunk a large dataset and load it to DB

## Workflow:
### Stage 1
- Load config from file, which has info where dumps and dump extensions are configured
- For every JSON file in dump there should be a subsequent mother/parent table available in database which helps us to create child or version tables using PostgreSQL inheritence
- If not available, ingestion job creates the mother tables at the start of the process

```python
    cfg = refresh_config()
    cnx = pgconnector(cfg['dburi'])
    mother_tables(cnx, 'recreate')
```
- Function **mother_tables** in **mother_tables.py** is a simple function to create or purge tables

### Stage 2
- Scan for the compressed files to be ingested and queue up

```python
# File Scanner function in zipops.py helps us fetch the dump files
    file_dumps = file_scanner(cfg['xport_cfg']['zip_path'])

# Function to get active tables from database
    active_tables = get_active_tables(cnx)
```

- _ProcessPoolExecutor_ spins up the configured number of processes and scans the compressed dumps for JSON files to prepare the list of tables to be created and loaded

```python
    with ProcessPoolExecutor(max_workers=cpu_workers) as executor:
        pool_dict = {
            executor.submit(
                build_file_set,
                cfg,
                dump,
                active_tables
            ): dump for dump in file_set
        }
    file_catalogue = list()
    for _future in as_completed(pool_dict):
        file_catalogue.extend(_future.result())
```

- Each process pooled from _ProcessPoolExecutor_ executes the function **build_file_set** with common arguments of `cfg` and `active_tables` and loop each dump file present in python list `file_set`. <br> For instance, we have configured 3 processes in YML file, so three processes will be created in the pool and each pool process will pick one element in the python list `file_set`
- Function **build_file_set** in **zipops.py** creates list of dicts which has meta information on version of the table, location of table data in dump file and other useful info which helps us to create the version table and ingest data

### Stage 3
- ProcessPoolExecutor is again engaged to spin up processes, loop through the list and access data files inside zip file and ingest data to tables. Logic developed using this [gist](https://gist.github.com/anacrolix/3788413)

Code Extensibility:
- At present, it is designed to work with compressed dumps created using SQL Server(MSSQL) and MySQL. It can be extended to work with dumps created using other databases by creating a JSON file for datatypes and configuring the data format like NULL pattern, data separator and encapsulation in YML file
- You can create decorators to track and record the progress/errors of the ingestion
- Schedule the ingestion job to a job scheduler
- GPG file encryption could be better option in term of security, python's gnupg library helps us to deal with encrypted files
- With few modifications to the code, we can also fetch data directly from database using the libraries
