DROP SCHEMA IF EXISTS framework CASCADE;

CREATE SCHEMA framework;

CREATE TABLE framework.jobs(
  jobid TEXT,
  status BOOL,
  jobname TEXT,
  joborder SMALLINT,
  biz_starttime TIMESTAMP WITHOUT TIME ZONE,
  biz_endtime TIMESTAMP WITHOUT TIME ZONE,
  latency_bizhrs INT,
  latency_weekend INT,
  latency_nonbizhrs INT
);

CREATE TABLE framework.tracker(
  pid BIGINT,
  jobid TEXT,
  starttime TIMESTAMP WITHOUT TIME ZONE,
  endtime TIMESTAMP WITHOUT TIME ZONE,
  notes TEXT
);

CREATE TABLE framework.instanceconfig (
    instanceid INT GENERATED ALWAYS AS IDENTITY,
    isactive BOOL,
    instancetype TEXT,
    instancecode TEXT,
    hostip TEXT,
    hport TEXT,
    uid TEXT,
    pwd TEXT,
    dbname TEXT,
	instancedesc TEXT
);

CREATE TABLE framework.collectiontracker(
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

CREATE TABLE framework.collectionmaps(
  cid INT GENERATED ALWAYS AS IDENTITY,
  isactive BOOL,
  instancetype TEXT,
  collection TEXT,
  staging_table TEXT
);

CREATE TABLE framework.chunktraces(
  logid INT GENERATED ALWAYS AS IDENTITY,
  status BOOL,
  pid BIGINT,
  instancecode TEXT,
  instancetype TEXT,
  collection TEXT,
  rowversion BIGINT,
  timestarted TIMESTAMP WITHOUT TIME ZONE,
  timefinished TIMESTAMP WITHOUT TIME ZONE
);

CREATE TABLE framework.cachetraces(
  cacheid BIGINT GENERATED ALWAYS AS IDENTITY,
  pid BIGINT,
  instancecode TEXT,
  collection TEXT,
  cachecount BIGINT,
  starttime TIMESTAMP WITHOUT TIME ZONE,
  endtime TIMESTAMP WITHOUT TIME ZONE
);

CREATE TABLE framework.volatilecollections AS
SELECT fic.instancecode,fic.instancetype,fcm.collection,fcm.staging_table ,0::bigint as rover
FROM framework.instanceconfig fic
JOIN framework.collectionmaps fcm ON fic.instancetype=fcm.instancetype
	WHERE fcm.isactive=true AND fic.isactive=true;

CREATE OR REPLACE FUNCTION framework.activecollections()
    RETURNS TABLE(icode TEXT,instancetype TEXT,collection TEXT,s_table TEXT,rower BIGINT) AS
$$
BEGIN

TRUNCATE TABLE framework.volatilecollections;
INSERT INTO framework.volatilecollections
SELECT fic.instancecode,fic.instancetype,fcm.collection,fcm.staging_table ,0::BIGINT AS rover
  FROM framework.instanceconfig fic
  JOIN framework.collectionmaps fcm ON fic.instancetype=fcm.instancetype
    WHERE fcm.isactive=true AND fic.isactive=true;
UPDATE framework.volatilecollections AS fin
SET rover=cht.rowversion
FROM (SELECT fct.instancecode,fct.instancetype,fct.collection,fct.rowversion,
      RANK() OVER(PARTITION BY fct.collection,fct.instancetype,fct.instancecode ORDER BY fct.rowversion DESC)::BIGINT AS nrank
  	   FROM framework.collectiontracker fct
  		   WHERE fct.status=true)cht
WHERE fin.instancecode=cht.instancecode AND fin.instancetype=cht.instancetype AND fin.collection=cht.collection AND cht.nrank=1;
RETURN QUERY SELECT * FROM framework.volatilecollections;

END;
$$
LANGUAGE plpgsql VOLATILE
COST 100;

INSERT INTO framework.instanceconfig(isactive,instancetype,instancecode,hostip,hport,uid,pwd,dbname,instancedesc)
SELECT true, 'mssql', 'devqa', '10.200.0.42\DEVSQL', 49887, 'lmsreaduser', 'lmsreaduser', 'iLMSDEV_REPL', 'local replica'
UNION ALL
SELECT true, 'mssql', 'local', '10.200.0.42\DEVSQL', 49887, 'lmsreaduser', 'lmsreaduser', 'iLMSDEV_REPL', 'local replica'
UNION ALL
SELECT true, 'mysql', 'local', '10.200.0.42\DEVSQL', 1433, 'lmsreaduser', 'lmsreaduser', 'iLMSDEV_REPL', 'local replica';

INSERT INTO framework.collectionmaps(isactive,instancetype,collection,staging_table)
SELECT false,'mssql','tbl_user_master','s_user_master'
UNION ALL
SELECT true,'mssql','tbl_customer_lookup','s_customer_lookup'
UNION ALL
SELECT true,'mysql','emailcamps','s_email'
UNION ALL
SELECT false,'mssql','tbl_course_name','s_course_name'
UNION ALL
SELECT false,'mssql','tbl_customer_course','s_customer_course'
UNION ALL
SELECT false,'mssql','tbl_student_lookup','s_student_lookup'
UNION ALL
SELECT false,'mssql','tbl_rulegroup_user','s_rulegroup_user'
UNION ALL
SELECT false,'mssql','tbl_assign_course_student','s_assign_course_student'
UNION ALL
SELECT false,'mssql','tbl_Department_Lookup','s_department_lookup'
UNION ALL
SELECT false,'mssql','tbl_Division_Lookup','s_division_lookup'
UNION ALL
SELECT false,'mssql','tbl_Region_Lookup','s_region_lookup'
