DROP SCHEMA IF EXISTS nudev CASCADE;
DROP SCHEMA IF EXISTS framework CASCADE;

CREATE SCHEMA nudev;
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

CREATE TABLE framework.datatypes(
  mssql text,
  mysql text,
  pgsql text
);

CREATE TABLE framework.tabshape(
  instancecode TEXT,
  collection TEXT,
  datatype TEXT,
  column_str TEXT
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
  stage_table TEXT,
  stage_fields TEXT,
  pki_table TEXT,
  pki_fields TEXT
);

CREATE TABLE framework.chunktraces(
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

CREATE TABLE framework.cachetraces(
  cacheid BIGINT GENERATED ALWAYS AS IDENTITY,
  pid BIGINT,
  instancecode TEXT,
  collection TEXT,
  primekeys TEXT,
  kount BIGINT,
  starttime TIMESTAMP WITHOUT TIME ZONE,
  endtime TIMESTAMP WITHOUT TIME ZONE
);

CREATE TABLE framework.tabshapetraces(
  epoch DOUBLE PRECISION DEFAULT EXTRACT(epoch FROM CURRENT_TIMESTAMP),
  tabquery TEXT
);

CREATE TABLE framework.volatilecollections AS
SELECT fic.instancecode,fic.instancetype,fcm.collection,fcm.stage_table,fcm.stage_fields,fcm.pki_table,fcm.pki_fields ,0::bigint as rover
FROM framework.instanceconfig fic
JOIN framework.collectionmaps fcm ON fic.instancetype=fcm.instancetype
	WHERE fcm.isactive=true AND fic.isactive=true;

CREATE OR REPLACE FUNCTION framework.activecollections()
    RETURNS TABLE(icode TEXT,instancetype TEXT,collection TEXT,s_table TEXT,stg_cols TEXT,pkitab TEXT,pki_cols TEXT,rower BIGINT) AS
$$
BEGIN

TRUNCATE TABLE framework.volatilecollections;
INSERT INTO framework.volatilecollections
SELECT fic.instancecode,fic.instancetype,fcm.collection,fcm.stage_table,fcm.stage_fields,fcm.pki_table,fcm.pki_fields ,0::BIGINT AS rover
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

CREATE OR REPLACE FUNCTION framework.createcollection(srctab TEXT, dsttab TEXT,schemaname TEXT) RETURNS TEXT
AS
$$
    DECLARE
        qry TEXT='CREATE TABLE IF NOT EXISTS ';
        cols TEXT;
    BEGIN
        SELECT string_agg(result,',') as q INTO cols FROM(
        SELECT array_to_string(ARRAY[col_name ,pg_dt], ' ')  AS result
        FROM(SELECT fts.column_str as col_name,fdt.pgsql as pg_dt
        FROM framework.datatypes fdt
        JOIN framework.tabshape fts ON fdt.mssql=fts.datatype
        WHERE fts.collection=srctab
        )A)X;
        qry=CONCAT(qry,schemaname,'.',dsttab,'(instancecode TEXT,',cols,',sys_rowversion TEXT,rower BIGINT)');
        INSERT INTO framework.tabshapetraces(tabquery) SELECT qry;
        EXECUTE qry;
        RETURN qry;
    END;
$$
LANGUAGE plpgsql VOLATILE
COST 100;

INSERT INTO framework.instanceconfig(isactive,instancetype,instancecode,hostip,hport,uid,pwd,dbname,instancedesc)
SELECT false, 'mssql', 'devqa', '10.200.0.42\DEVSQL', 49887, 'lmsreaduser', 'lmsreaduser', 'iLMSDEV_REPL', 'local replica'
UNION ALL
SELECT true, 'mssql', 'local', '10.200.0.42\DEVSQL', 49887, 'lmsreaduser', 'lmsreaduser', 'iLMSDEV_REPL', 'local replica'
UNION ALL
SELECT false, 'mysql', 'local', '10.200.0.42\DEVSQL', 1433, 'lmsreaduser', 'lmsreaduser', 'iLMSDEV_REPL', 'local replica';

INSERT INTO framework.datatypes
SELECT 'bigint','','BIGINT'
UNION ALL
SELECT 'binary','','TEXT'
UNION ALL
SELECT 'bit','','BOOL'
UNION ALL
SELECT 'char','','TEXT'
UNION ALL
SELECT 'date','','DATE'
UNION ALL
SELECT 'datetime','','TIMESTAMP WITHOUT TIME ZONE'
UNION ALL
SELECT 'decimal','','FLOAT'
UNION ALL
SELECT 'float','','FLOAT'
UNION ALL
SELECT 'image','','TEXT'
UNION ALL
SELECT 'int','','BIGINT'
UNION ALL
SELECT 'nchar','','TEXT'
UNION ALL
SELECT 'ntext','','TEXT'
UNION ALL
SELECT 'numeric','','FLOAT'
UNION ALL
SELECT 'nvarchar','','TEXT'
UNION ALL
SELECT 'real','','FLOAT'
UNION ALL
SELECT 'smallint','','SMALLINT'
UNION ALL
SELECT 'text','','TEXT'
UNION ALL
SELECT 'tinyint','','SMALLINT'
UNION ALL
SELECT 'uniqueidentifier','','TEXT'
UNION ALL
SELECT 'varbinary','','TEXT'
UNION ALL
SELECT 'varchar','','TEXT'
UNION ALL
SELECT 'xml','','TEXT';

INSERT INTO framework.collectionmaps(isactive,instancetype,collection,stage_table,stage_fields,pki_table,pki_fields)
SELECT true,'mssql','tbl_user_master','tbl_user_master','*','tbl_user_master_pki','flduserid'
UNION ALL
SELECT true,'mssql','tbl_customer_lookup','tbl_customer_lookup','*','tbl_customer_lookup_pki','fldcustid'
UNION ALL
SELECT true,'mysql','emailcamps','emailcamps','*','emailcamps_pki',''
UNION ALL
SELECT true,'mssql','tbl_course_name','tbl_course_name','*','tbl_course_name_pki','flduserdefinedcourseid'
UNION ALL
SELECT true,'mssql','tbl_customer_course','tbl_customer_course','*','tbl_customer_course_pki','intcustid,struserdefinedcourseid'
UNION ALL
SELECT true,'mssql','tbl_student_lookup','tbl_student_lookup','*','tbl_student_lookup_pki','flduserid,fldcustid'
UNION ALL
SELECT true,'mssql','tbl_rulegroup_user','tbl_rulegroup_user','*','tbl_rulegroup_user_pki','intrulegroupid'
UNION ALL
SELECT true,'mssql','tbl_assign_course_student','tbl_assign_course_student','*','tbl_assign_course_student_pki','fldassigncourseid,flduseridassignedto,struserdefinedcourseid'
UNION ALL
SELECT true,'mssql','tbl_department_lookup','tbl_department_lookup','*','tbl_department_lookup_pki','flddeptid'
UNION ALL
SELECT true,'mssql','tbl_division_lookup','tbl_division_lookup','*','tbl_division_lookup_pki','flddivisionid'
UNION ALL
SELECT true,'mssql','tbl_region_lookup','tbl_region_lookup','*','tbl_region_lookup_pki','fldregionid'
UNION ALL
SELECT true,'mssql','tbl_rulegroup_master','tbl_rulegroup_master','*','tbl_rulegroup_master_pki','intrulegroupid';

UPDATE framework.collectionmaps set stage_fields= 'fldUserDefinedCourseId,fldCourseName,fldUsageLimitation,fldSendReminder1,fldSendReminder2,fldReminderDays1,fldReminderDays2,fldCourseDuration,fldSetLicense,fldDeliveryMethod,charCourseType,fldDisplayCourseid,fldOwnerCustId,intLicensePeriod,charCourseStatus,charAllowEnrollCourse,convert(VARCHAR,strPrerequisitesUserDefinedCourseIds) as strprerequisitesuserdefinedcourseids ,strCourseCurriculumCategory,intGroupId,bitSendCertificateOnCourseCompletion,strSignatureFilePath,strLeftSignatureFilePath,strHeaderSignatureFilePath,strFooterSignatureFilePath,charDisplaySurvey,charIsSurveyMandatory,charIsVersioningOff,charIsMultiModuleICCourse,charIsCertificationProgramEnable,decPrice,charLicensePeriodType,bitIsEnableLicensePeriod,intCertificationValidPeriod,charCertificationCriteriaField,charIsSendReminderAndReEnrollInCertification,intFirstCertificationReminder,charReEnrollInCertification,charIsSecondCertificationReminderEnable,intSecondCertificationReminder,charIsThirdCertificationReminderEnable,intThirdCertificationReminder,charIsGracePeriodEnable,intGracePeriodDays,charIsSendGracePeriodStartNotification,charSendMessageToUser,charSendMessageToManager,charSendMessageToOrgAdministrator,CharIsSendMessageToOtherEnable,charSendMessageToOther,strUserDefinedCourseIDToEnrollAfterExpiration,charAllowCourseAccessAfterExpirationAndGracePeriod,bitIsLockEnabled,dtmCourseDurationSpecific,bitPastDueSendEmailUsr,bitPastDueLockCourse,bitPastDueSendEmailMgr,charPastDueChangeStatus,IsScormEngineCourse,charSendExpiredMessage,bitSendCertificateHeaderText,bitSendCertificateFooterText,bitEnablePastDueChangeStatus,charIsSendExpiredMessage,charIsSkipCourseStatusScreen,EmbedCount,DisplayCertificateOnUserTranscript,strCourseDirectoryName,charWindowSize,intWindowWidth,intWindowHeight,bitPreventWindowResize,bitIsMEEnable,bitIsCertificationEnable,bitIsAssignedViaOtherEnrollment,IsIELSCORMCourse,bitIfSAChangeLicense,bitIsHistorical,bitIsEnableVolumeDiscount,bitIsSendLicenseExpirationMail,intDaysBeforeLicenseExpiration,tinyintIEngineVersion,dtmCreatedDate,bitIsNew,chariEngineCourseType,bitIsLogoBranding,bitIsPolicyBranding,charDueDateUserProfileField,strCopiedFromUserDefinedCourseID,charCourseImportType,bitIsMobileEnabled,bitIsMobileReady,bitIsMobileOffline,bitIsCyQ'
where collection='tbl_course_name' and instancetype='mssql';

update framework.collectionmaps set isactive=false;
update framework.collectionmaps set isactive=true where collection in ('tbl_customer_lookup','tbl_department_lookup','tbl_region_lookup');

select * from framework.activecollections();
