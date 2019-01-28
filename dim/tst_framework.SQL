DROP SCHEMA IF EXISTS framework CASCADE;

CREATE SCHEMA framework;

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

INSERT INTO framework.instanceconfig(isactive,instancetype,instancecode,hostip,hport,uid,pwd,dbname,instancedesc)
SELECT true, 'mssql', 'devqa', 'SERVER2\DEVSQL', 1433, 'lmsreaduser', 'lmsreaduser', 'iLMSDEV_REPL', 'local replica'
UNION ALL
SELECT true, 'mssql', 'local-qa', 'SERVER2\DEVSQL', 1433, 'lmsreaduser', 'lmsreaduser', 'iLMSDEV_REPL', 'local replica'
UNION ALL
SELECT true, 'mssql', 'local', 'SERVER2\DEVSQL', 1433, 'lmsreaduser', 'lmsreaduser', 'iLMSDEV_REPL', 'local replica'
UNION ALL
SELECT true, 'mysql', 'local', 'SERVER2\DEVSQL', 1433, 'lmsreaduser', 'lmsreaduser', 'iLMSDEV_REPL', 'local replica';

CREATE TABLE framework.changetracker(
  changeid INT GENERATED ALWAYS AS IDENTITY,
  status BOOL,
  instancecode TEXT,
  instancetype TEXT,
  collection TEXT,
  rowversion BIGINT,
  timestarted TIMESTAMP WITHOUT TIME ZONE,
  timefinished TIMESTAMP WITHOUT TIME ZONE,
  timeupdated TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP

);

CREATE TABLE framework.collectionmaps(
  isactive BOOL,
  instancetype TEXT,
  collection TEXT,
  staging_table TEXT
);

CREATE TABLE IF NOT EXISTS framework.jobcollections AS
SELECT fic.instancecode,fic.instancetype,fcm.collection,fcm.staging_table ,0::bigint as rover
FROM framework.instanceconfig fic
JOIN framework.collectionmaps fcm ON fic.instancetype=fcm.instancetype
	WHERE fcm.isactive=true AND fic.isactive=true;

  CREATE OR REPLACE FUNCTION framework.activecollections()
    RETURNS TABLE(icode TEXT,instancetype TEXT,collection TEXT,s_table TEXT,rower BIGINT) AS
  $BODY$
  BEGIN

  truncate table framework.jobcollections;
  insert into framework.jobcollections
  SELECT fic.instancecode,fic.instancetype,fcm.collection,fcm.staging_table ,0::bigint as rover
  FROM framework.instanceconfig fic
  JOIN framework.collectionmaps fcm ON fic.instancetype=fcm.instancetype
  	WHERE fcm.isactive=true AND fic.isactive=true;
  update framework.jobcollections as fin
  set rover=cht.rowversion
  from (SELECT fct.instancecode,fct.instancetype,fct.collection,fct.rowversion,
  	RANK() OVER(PARTITION BY fct.collection,fct.instancetype,fct.instancecode ORDER BY fct.rowversion DESC)::BIGINT as nrank
  	FROM framework.changetracker fct
  		WHERE fct.status=true)cht
  where fin.instancecode=cht.instancecode and fin.instancetype=cht.instancetype and fin.collection=cht.collection and cht.nrank=1;
  return query select * from framework.jobcollections;
  END;
  $BODY$
    LANGUAGE plpgsql VOLATILE
    COST 100;

INSERT INTO framework.changetracker(status,instancecode,instancetype,collection,rowversion)
SELECT true,'devqa','mssql','tbl_user_master',18080
UNION ALL
SELECT true,'devqa','mssql','tbl_user_master',18079
UNION ALL
SELECT true,'devqa','mysql','emailcamps',1666
UNION ALL
SELECT false,'devqa','mssql','emailcamps',1777
UNION ALL
SELECT true,'local','mysql','emailcamps',4444
UNION ALL
SELECT true,'local','mssql','tbl_user_master',55555;

INSERT INTO framework.collectionmaps
SELECT true,'mssql','tbl_user_master','staging.s_user_master'
UNION ALL
SELECT true,'mssql','tbl_customer_lookup','staging.s_customer_lookup'
UNION ALL
SELECT true,'mysql','emailcamps','staging.s_email'
UNION ALL
SELECT true,'mssql','tbl_course_name','staging.s_course_name'
UNION ALL
SELECT true,'mssql','tbl_customer_course','staging.s_customer_course'
UNION ALL
SELECT true,'mssql','tbl_student_lookup','staging.s_student_lookup'
UNION ALL
SELECT true,'mssql','tbl_rulegroup_user','staging.s_rulegroup_user'
