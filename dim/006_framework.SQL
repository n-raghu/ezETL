CREATE SCHEMA IF NOT EXISTS framework;

CREATE TABLE IF NOT EXISTS framework.jobs(
  status BOOL,
  jobid TEXT,
  launcher TEXT,
  jobname TEXT,
  joborder SMALLINT,
  biz_starttime TIME WITHOUT TIME ZONE,
  biz_endtime TIME WITHOUT TIME ZONE,
  latency_bizhrs INT,
  latency_nonbizhrs INT
);

CREATE TABLE IF NOT EXISTS framework.tracker(
  pid BIGINT,
  jobid TEXT,
  starttime TIMESTAMP WITHOUT TIME ZONE,
  endtime TIMESTAMP WITHOUT TIME ZONE,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS framework.datatypes(
  mssql text,
  mysql text,
  pgsql text
);

CREATE TABLE IF NOT EXISTS framework.tabshape(
  instancecode TEXT,
  app TEXT,
  collection TEXT,
  datatype TEXT,
  column_str TEXT
);

CREATE TABLE IF NOT EXISTS framework.instanceconfig (
    instanceid INT GENERATED ALWAYS AS IDENTITY,
    isactive BOOL,
    app TEXT,
    instancetype TEXT,
    instancecode TEXT,
    hostip TEXT,
    hport TEXT,
    uid TEXT,
    pwd TEXT,
    dbname TEXT,
	instancedesc TEXT
);

CREATE TABLE IF NOT EXISTS framework.collectiontracker(
  changeid INT GENERATED ALWAYS AS IDENTITY,
  status BOOL,
  pid BIGINT,
  instancecode TEXT,
  instancetype TEXT,
  collection TEXT,
  rowversion BIGINT,
  timestarted TIMESTAMP WITHOUT TIME ZONE,
  timefinished TIMESTAMP WITHOUT TIME ZONE,
  epoch DOUBLE PRECISION DEFAULT EXTRACT(epoch FROM CURRENT_TIMESTAMP)
);

CREATE TABLE IF NOT EXISTS framework.collectionmaps(
  cid INT GENERATED ALWAYS AS IDENTITY,
  isactive BOOL,
  app TEXT,
  instancetype TEXT,
  collection TEXT,
  stage_table TEXT,
  stage_fields TEXT,
  pki_table TEXT,
  pki_fields TEXT
);

CREATE TABLE IF NOT EXISTS framework.errorlogs(
  pid BIGINT,
  jobid TEXT,
  error TEXT,
  error_time TIMESTAMP WITHOUT TIME ZONE,
  db_timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS framework.chunktraces(
  logid INT GENERATED ALWAYS AS IDENTITY,
  status BOOL,
  pid BIGINT,
  instancecode TEXT,
  instancetype TEXT,
  collection TEXT,
  rowversion BIGINT,
  chunkstart TIMESTAMP WITHOUT TIME ZONE,
  chunkfinish TIMESTAMP WITHOUT TIME ZONE
);

CREATE TABLE IF NOT EXISTS framework.cachetraces(
  cacheid BIGINT GENERATED ALWAYS AS IDENTITY,
  pid BIGINT,
  status BOOL,
  instancecode TEXT,
  collection TEXT,
  primekeys TEXT,
  kount BIGINT,
  starttime TIMESTAMP WITHOUT TIME ZONE,
  endtime TIMESTAMP WITHOUT TIME ZONE
);

CREATE TABLE IF NOT EXISTS framework.tabshapetraces(
  epoch DOUBLE PRECISION DEFAULT EXTRACT(epoch FROM CURRENT_TIMESTAMP),
  tabquery TEXT
);

CREATE TABLE IF NOT EXISTS framework.stakeholders(
  recepient TEXT,
  recepient_info TEXT,
  active BOOL,
  tolist BOOL,
  cclist BOOL,
  last_mod_time TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS framework.api_traces(
  app TEXT,
  endpoint TEXT,
  headers TEXT,
  params TEXT,
  requesttime TEXT,
  responsecode TEXT,
  responseok BOOL
);

CREATE TABLE IF NOT EXISTS framework.api_salesforce_tracker(
  changeid INT GENERATED ALWAYS AS IDENTITY,
  pid BIGINT,
  collection TEXT,
  start_date DATE,
  end_date DATE,
  epoch DOUBLE PRECISION DEFAULT EXTRACT(epoch FROM CURRENT_TIMESTAMP)
);

CREATE TABLE IF NOT EXISTS framework.api_salesforce_mappers(
  active BOOL,
  mapid INT GENERATED ALWAYS AS IDENTITY,
  sfdc_api TEXT,
  sfdc_collection TEXT,
  sfdc_filters TEXT,
  sfdc_column TEXT,
  collection TEXT,
  last_mod_time TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS framework.statements(
  isactive BOOL,
  statement_id INT GENERATED ALWAYS AS IDENTITY,
  statement TEXT,
  created DATE DEFAULT CURRENT_DATE
);

CREATE TABLE IF NOT EXISTS framework.volatiletracker AS
SELECT * FROM framework.tracker;

CREATE TABLE IF NOT EXISTS framework.volatilecollections AS
SELECT fcm.app,fic.instancecode,fic.instancetype,fcm.collection,fcm.stage_table,fcm.stage_fields,fcm.pki_table,fcm.pki_fields ,0::bigint as rover
FROM framework.instanceconfig fic
JOIN framework.collectionmaps fcm ON fic.instancetype=fcm.instancetype
	WHERE fcm.isactive=true AND fic.isactive=true;
