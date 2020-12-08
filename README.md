I would like to share one of the fastest & automated way of ingesting different versions of application tables dump to database, we achieved this using generators to lazy load, concurrent.futures.ProcessPoolExecutor to load data in parallel and PostgreSQL inheritence concept to maintain different versions of the application.

For this demo, I have two ZIP files which are a dump of same application but different database versions.
ZIP File v1 has 3 JSON files and 3 data files. The same goes with v2.
JSON files represent schema of the table and DAT file are a kind of flat file

What changes from v1 to v2?
    - Application features change time over time which also changes the schema of the underlying database, however, while deploying teams choose to deploy the changes region by region i.e. home countries/region experiences the changes earlier than other regions.
    - Problems arise when you're analyzing/dashboarding complete aggregated data of all regions onto a single pane.

Code Extensibility:
    - At present, it is designed to work with compressed dumps created using SQL Server(MSSQL) and MySQL. It can be extended to work with dumps created using other databases by creating a JSON file for datatypes and configuring the data format like NULL pattern, data separator and encapsulation in YML file
    - You can create decorators to track and record the progress/errors of the ingestion
    - Schedule the ingestion job to a job scheduler
    - GPG file encryption could be better option in term of security, python's gnupg library helps us to deal with encrypted files
    - With few modifications to the code, we can also fetch data directly from database using the libraries

Workflow:
    - For every JSON file in dump there should be a subsequent mother/parent table available in database which helps us to create child or version tables using PostgreSQL inheritence
    - If not available, ingestion job creates the mother tables at the start of the process
    - ProcessPoolExecutor spins up the configured number of processes and scans the compressed dumps for JSON files to prepare the list of tables to be created and loaded
    - ProcessPoolExecutor is again engaged to spin up processes, loop through the list and access data files inside zip file and ingest data to tables

Working Principle:
    - Python iterator to chunk a large dataset and load it to DB
    - zipfile.ZipFile class allows to access and read files inside a compressed file
    - ProcessPoolExecutor to churn up processes for ingestion and loop the catalogue
