DROP SCHEMA IF EXISTS devstage CASCADE;
DROP SCHEMA IF EXISTS framework CASCADE;

CREATE SCHEMA devstage;
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
  staging_table TEXT,
  stage_cols TEXT,
  pkitable TEXT
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
  primekeys TEXT,
  cachecount BIGINT,
  starttime TIMESTAMP WITHOUT TIME ZONE,
  endtime TIMESTAMP WITHOUT TIME ZONE
);

CREATE TABLE framework.volatilecollections AS
SELECT fic.instancecode,fic.instancetype,fcm.collection,fcm.staging_table,fcm.pkitable,fcm.stage_cols ,0::bigint as rover
FROM framework.instanceconfig fic
JOIN framework.collectionmaps fcm ON fic.instancetype=fcm.instancetype
	WHERE fcm.isactive=true AND fic.isactive=true;

CREATE OR REPLACE FUNCTION framework.activecollections()
    RETURNS TABLE(icode TEXT,instancetype TEXT,collection TEXT,s_table TEXT,pkitab TEXT,stg_cols TEXT,rower BIGINT) AS
$$
BEGIN

TRUNCATE TABLE framework.volatilecollections;
INSERT INTO framework.volatilecollections
SELECT fic.instancecode,fic.instancetype,fcm.collection,fcm.staging_table,fcm.pkitable,fcm.stage_cols ,0::BIGINT AS rover
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
SELECT false, 'mssql', 'devqa', '10.200.0.42\DEVSQL', 49887, 'lmsreaduser', 'lmsreaduser', 'iLMSDEV_REPL', 'local replica'
UNION ALL
SELECT true, 'mssql', 'local', '10.200.0.42\DEVSQL', 49887, 'lmsreaduser', 'lmsreaduser', 'iLMSDEV_REPL', 'local replica'
UNION ALL
SELECT false, 'mysql', 'local', '10.200.0.42\DEVSQL', 1433, 'lmsreaduser', 'lmsreaduser', 'iLMSDEV_REPL', 'local replica';

INSERT INTO framework.collectionmaps(isactive,instancetype,collection,staging_table,stage_cols,pkitable)
SELECT true,'mssql','tbl_user_master','tbl_user_master','*','tbl_user_master_pki'
UNION ALL
SELECT true,'mssql','tbl_customer_lookup','tbl_customer_lookup','*','tbl_customer_lookup_pki'
UNION ALL
SELECT true,'mysql','emailcamps','emailcamps','*','emailcamps_pki'
UNION ALL
SELECT true,'mssql','tbl_course_name','tbl_course_name','*','tbl_course_name_pki'
UNION ALL
SELECT true,'mssql','tbl_customer_course','tbl_customer_course','*','tbl_customer_course_pki'
UNION ALL
SELECT true,'mssql','tbl_student_lookup','tbl_student_lookup','*','tbl_student_lookup_pki'
UNION ALL
SELECT true,'mssql','tbl_rulegroup_user','tbl_rulegroup_user','*','tbl_rulegroup_user_pki'
UNION ALL
SELECT true,'mssql','tbl_assign_course_student','tbl_assign_course_student','*','tbl_assign_course_student_pki'
UNION ALL
SELECT true,'mssql','tbl_department_lookup','tbl_department_lookup','*','tbl_department_lookup_pki'
UNION ALL
SELECT true,'mssql','tbl_division_lookup','tbl_division_lookup','*','tbl_division_lookup_pki'
UNION ALL
SELECT true,'mssql','tbl_region_lookup','tbl_region_lookup','*','tbl_region_lookup_pki'
UNION ALL
SELECT true,'mssql','tbl_rulegroup_master','tbl_rulegroup_master','*','tbl_rulegroup_master_pki';

UPDATE framework.collectionmaps set stage_cols= 'fldUserDefinedCourseId,fldCourseName,fldUsageLimitation,fldSendReminder1,fldSendReminder2,fldReminderDays1,fldReminderDays2,fldCourseDuration,fldSetLicense,fldDeliveryMethod,charCourseType,fldDisplayCourseid,fldOwnerCustId,intLicensePeriod,charCourseStatus,charAllowEnrollCourse,convert(VARCHAR,strPrerequisitesUserDefinedCourseIds) as strprerequisitesuserdefinedcourseids ,strCourseCurriculumCategory,intGroupId,bitSendCertificateOnCourseCompletion,strSignatureFilePath,strLeftSignatureFilePath,strHeaderSignatureFilePath,strFooterSignatureFilePath,charDisplaySurvey,charIsSurveyMandatory,charIsVersioningOff,charIsMultiModuleICCourse,charIsCertificationProgramEnable,decPrice,charLicensePeriodType,bitIsEnableLicensePeriod,intCertificationValidPeriod,charCertificationCriteriaField,charIsSendReminderAndReEnrollInCertification,intFirstCertificationReminder,charReEnrollInCertification,charIsSecondCertificationReminderEnable,intSecondCertificationReminder,charIsThirdCertificationReminderEnable,intThirdCertificationReminder,charIsGracePeriodEnable,intGracePeriodDays,charIsSendGracePeriodStartNotification,charSendMessageToUser,charSendMessageToManager,charSendMessageToOrgAdministrator,CharIsSendMessageToOtherEnable,charSendMessageToOther,strUserDefinedCourseIDToEnrollAfterExpiration,charAllowCourseAccessAfterExpirationAndGracePeriod,bitIsLockEnabled,dtmCourseDurationSpecific,bitPastDueSendEmailUsr,bitPastDueLockCourse,bitPastDueSendEmailMgr,charPastDueChangeStatus,IsScormEngineCourse,charSendExpiredMessage,bitSendCertificateHeaderText,bitSendCertificateFooterText,bitEnablePastDueChangeStatus,charIsSendExpiredMessage,charIsSkipCourseStatusScreen,EmbedCount,DisplayCertificateOnUserTranscript,strCourseDirectoryName,charWindowSize,intWindowWidth,intWindowHeight,bitPreventWindowResize,bitIsMEEnable,bitIsCertificationEnable,bitIsAssignedViaOtherEnrollment,IsIELSCORMCourse,bitIfSAChangeLicense,bitIsHistorical,bitIsEnableVolumeDiscount,bitIsSendLicenseExpirationMail,intDaysBeforeLicenseExpiration,tinyintIEngineVersion,dtmCreatedDate,bitIsNew,chariEngineCourseType,bitIsLogoBranding,bitIsPolicyBranding,charDueDateUserProfileField,strCopiedFromUserDefinedCourseID,charCourseImportType,bitIsMobileEnabled,bitIsMobileReady,bitIsMobileOffline,bitIsCyQ'
where collection='tbl_course_name' and instancetype='mssql'
