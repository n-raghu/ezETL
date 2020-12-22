I would like to share one of the fastest & automated way of ingesting different versions of application tables dump to database, we achieved this using generators to lazy load, concurrent.futures.ProcessPoolExecutor to load data in parallel and PostgreSQL inheritence concept to maintain different versions of the application.

For this demo, I have two ZIP files which are a dump of same application but different database versions.
ZIP File v1 has 3 JSON files and 3 data files. The same goes with v2.
JSON files represent schema of the table and DAT file are a kind of flat file

## What changes from v1 to v2?

- Application features change time over time which also changes the schema of the underlying database, however, while deploying teams choose to deploy the changes region by region i.e. home countries/region experiences the changes earlier than other regions.
- Problems arise when you're analyzing/dashboarding complete aggregated data of all regions onto a single pane.
- To resolve this, we chose PostgreSQL Inheritence concept to maintain all versions of the applications in one single DB and works for dashboards with the common fields available in all versions.

## Working Principle

- ProcessPoolExecutor to churn up processes and scan for JSON files in each dump file 
- Prepare a catalogue of files across all compressed files to be ingested
- ProcessPoolExecutor uses processes from the pool to loop through the catalogue and ingestion data
- zipfile.ZipFile class allows to access and read files inside a compressed file
- Python iterator to chunk a large dataset and load it to DB

## Workflow
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
- This is the important and final stage of the ingestion job, where we read the data files inside compressed files and load them to tables.

```python
    with ProcessPoolExecutor(max_workers=cpu_workers) as executor:
        _i = {
            executor.submit(
                zip_to_tbl,
                dat_sep,
                quote_pattern,
                dburi,
                dtypes,
                one_set,
            ): one_set for one_set in file_catalogue
        }
```

- ProcessPoolExecutor is again engaged to spin up processes, loop through the list `file_catalogue` and provide as arguments to function **zip_to_tbl** along the with the default arguments
- Function **zip_to_tbl** is executed by each process spun by ProcessPoolExecutor to 
    - Read JSON files of each table and create dict(Refer object `tbl_json` in below code) which has schema of version table
    - Use version table schema dict as argument to function **create_ins_tbl** to create instance/version table
    - Function **get_csv_structure** helps us to read the order of columns available in the first line of a file
    - Prepare COPY statement using csv_structure and version table
    - Access data files, create chunks of data and load data tables using copy_expert

```python
def zip_to_tbl(
    dat_sep,
    quote_pattern,
    urx,
    dtypes,
    one_set,
):
    cnx = pgconnector(urx)
    tbl_json = fmt_to_json(
        zipset=one_set['dataset'],
        jsonfile=one_set['fmt_file'],
        dtdct=dtypes,
    )
    create_ins_tbl(
        cnx,
        mother_tbl=one_set['mother_tbl'],
        ins_tbl=one_set['ins_tbl'],
        ins_tbl_map=tbl_json,
    )
    null_pattern = 'NULL'
    tbl_header = get_csv_structure(
        zipset=one_set['dataset'],
        datfile=one_set['dat_file']
    )
    pg_cp_statement = f"""
                        COPY \
                            {one_set['ins_tbl']}({tbl_header}) \
                        FROM \
                            STDIN \
                        WITH \
                            CSV HEADER \
                            DELIMITER '{dat_sep}' \
                            NULL '{null_pattern}' \
                            QUOTE '{quote_pattern}' \
                    """
    with ZipFile(one_set['dataset'], 'r') as zfile:
        with zfile.open(one_set['dat_file'], 'r') as cfile:
            chunk = StrIOGenerator(cfile, text_enc='latin_1')
            with cnx.cursor() as dbcur:
                dbcur.copy_expert(sql=pg_cp_statement, file=chunk)
    cnx.commit()
    cnx.close()
```

_Note_: Logic used to create `chunk` feeds data from Iterator which reads data from file object and is developed based on this [gist](https://gist.github.com/anacrolix/3788413)

## Try out(Docker)
You can also check the execution of the utitlity on docker
- Pull the docker image
```git
git clone n-raghu/ezdba
```
- Run the compose file.
```git
docker-compose up -d
```
Compose file consists of two containers, One for the program and another for the PostgreSQL 12.1

- Once all the containers of compose file are up and running
```docker
docker exec -it k_pyx bash
cd /journal
python ingest_dat.py
```

### Sample Output

This will create sample mother tables and ingest data to version tables.
This will also spit out the PID and PPID of processes used to ingest data. This is to know how ProcessPoolExecutor works.


## Code Extensibility
- At present, it is designed to work with compressed dumps created using SQL Server(MSSQL) and MySQL. It can be extended to work with dumps created using other databases by creating a JSON file for datatypes and configuring the data format like NULL pattern, data separator and encapsulation in YML file
- You can create decorators to track and record the progress/errors of the ingestion. A sample decorator **timetracer** is created to record the time elapsed for executing a function. Refer function **timetracer** in **dimlib.py**.
- Schedule the ingestion job to a job scheduler
- GPG file encryption could be better option in term of security, python's gnupg library helps us to deal with encrypted files
- With few modifications to the code, we can also fetch data directly from database using the libraries
