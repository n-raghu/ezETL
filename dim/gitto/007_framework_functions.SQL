CREATE OR REPLACE FUNCTION framework.live_instancecollections()
    RETURNS TABLE(app TEXT,icode TEXT,instancetype TEXT,collection TEXT,s_table TEXT,stg_cols TEXT,pkitab TEXT,pki_cols TEXT,rower BIGINT) AS
$$
BEGIN

TRUNCATE TABLE framework.volatilecollections;
INSERT INTO framework.volatilecollections
SELECT fcm.app,fic.instancecode,fic.instancetype,fcm.collection,fcm.stage_table,fcm.stage_fields,fcm.pki_table,fcm.pki_fields ,0::BIGINT AS rover
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

CREATE OR REPLACE FUNCTION framework.createcollection(srctab TEXT, dsttab TEXT,schemaname TEXT,app_code TEXT) RETURNS TEXT
AS
$$
    DECLARE
        qry TEXT='CREATE UNLOGGED TABLE IF NOT EXISTS ';
        cols TEXT;
    BEGIN
        IF app_code='lms' THEN
          SELECT string_agg(result,',') as q INTO cols FROM(
          SELECT array_to_string(ARRAY[col_name ,pg_dt], ' ')  AS result
          FROM(SELECT DISTINCT fts.column_str as col_name,fdt.pgsql as pg_dt
          FROM framework.tabshape fts
          LEFT JOIN framework.datatypes fdt ON fdt.mssql=fts.datatype
          WHERE fts.collection=srctab AND fts.app=app_code
          )A)X;
        ELSE
          SELECT string_agg(result,',') as q INTO cols FROM(
          SELECT array_to_string(ARRAY[col_name ,pg_dt], ' ')  AS result
          FROM(SELECT DISTINCT fts.column_str as col_name,fdt.pgsql as pg_dt
          FROM framework.tabshape fts
          LEFT JOIN framework.datatypes fdt ON fdt.mysql=fts.datatype
          WHERE fts.collection=srctab AND fts.app=app_code
          )A)X;
        END IF;
        IF app_code='potter' THEN
          qry=CONCAT(qry,schemaname,'.',dsttab,'(',cols,',row_timestamp TIMESTAMP WITHOUT TIME ZONE,rower BIGINT)');
        ELSE
          qry=CONCAT(qry,schemaname,'.',dsttab,'(instancecode TEXT,',cols,',row_timestamp TIMESTAMP WITHOUT TIME ZONE,rower BIGINT)');
        END IF;
        INSERT INTO framework.tabshapetraces(tabquery) SELECT qry;
        EXECUTE qry;
        RETURN qry;
    END;
$$
LANGUAGE plpgsql VOLATILE
COST 100;

CREATE OR REPLACE FUNCTION framework.purgeduplicaterows(tabname TEXT, schemaname TEXT) RETURNS VOID AS
$$
    DECLARE
        cols TEXT;
        nustage TEXT=CONCAT(schemaname,'.',tabname,'_stg');
        drpqry TEXT=CONCAT('DROP TABLE IF EXISTS ',nustage);
        qry TEXT='CREATE TABLE ';
    BEGIN
        SELECT STRING_AGG(column_str,',') INTO cols FROM framework.tabshape WHERE collection=tabname;
        qry=CONCAT(qry,nustage,' AS SELECT * FROM ',schemaname,'.',tabname,' GROUP BY instancecode,',cols,',rower');
        EXECUTE drpqry;
        EXECUTE qry;
        EXECUTE CONCAT('DROP TABLE IF EXISTS ',schemaname,'.',tabname);
        EXECUTE CONCAT('ALTER TABLE ',nustage,' RENAME TO ',tabname);
    END;
$$
LANGUAGE plpgsql VOLATILE
COST 100;

CREATE OR REPLACE FUNCTION framework.launchpad() RETURNS TABLE(job_launcher TEXT,jid TEXT,job_order SMALLINT) AS
$$
	DECLARE
		ppid BIGINT=(SELECT COALESCE(MAX(PID),0) FROM framework.tracker);
	BEGIN
		IF (ppid<1) THEN
      TRUNCATE TABLE framework.volatiletracker;
			RETURN QUERY (SELECT DISTINCT launcher,jobid,joborder FROM framework.jobs WHERE status=true);
		END IF;
		CREATE TEMP TABLE bizschedules ON COMMIT DROP AS
		SELECT DISTINCT jobid,latency_bizhrs AS latency,0::int lastexecution,joborder,launcher
		,CASE WHEN CURRENT_TIME BETWEEN biz_starttime AND biz_endtime THEN 'biz' ELSE 'non_biz' END AS job_mode
		FROM framework.jobs
			WHERE status=true;
		UPDATE bizschedules AS bzs SET latency=fwj.latency_nonbizhrs
		FROM framework.jobs fwj
			WHERE bzs.jobid=fwj.jobid AND bzs.job_mode='non_biz';
		UPDATE bizschedules SET lastexecution=(
			SELECT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP -MIN(starttime)) FROM framework.tracker WHERE pid=ppid);
    IF (SELECT COUNT(*)>0 FROM bizschedules WHERE lastexecution>latency) THEN
      TRUNCATE TABLE framework.volatiletracker;
    END IF;
		RETURN QUERY (SELECT launcher,jobid,joborder FROM bizschedules WHERE lastexecution>latency);
	END;
$$
LANGUAGE plpgsql VOLATILE
COST 100;

CREATE OR REPLACE FUNCTION framework.updatetracker() RETURNS VOID AS
$$
BEGIN
    UPDATE framework.tracker AS fwt SET starttime=stg.starttime,endtime=stg.endtime,notes=stg.notes
    FROM (SELECT *,RANK() OVER(PARTITION BY jobid ORDER BY endtime DESC) AS nrank
        FROM framework.volatiletracker)stg
        WHERE fwt.pid=stg.pid AND fwt.jobid=stg.jobid AND nrank=1;
END
$$
LANGUAGE plpgsql VOLATILE
COST 100;
