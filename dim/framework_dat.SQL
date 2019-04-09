INSERT INTO framework.instanceconfig(isactive,app,instancetype,instancecode,hostip,hport,uid,pwd,dbname,instancedesc)
SELECT false, 'lms', 'mssql', 'devqa', '10.200.0.42\DEVSQL', 49887, 'lmsreaduser', 'lmsreaduser', 'iLMSDEV_REPL', 'local replica'
UNION ALL
SELECT false, 'lms', 'mssql', 'local', '10.200.0.42\DEVSQL', 49887, 'lmsreaduser', 'lmsreaduser', 'iLMSDEV_REPL', 'local replica'
UNION ALL
SELECT false, 'phishproof', 'mysql', 'local', '10.200.0.42\DEVSQL', 1433, 'lmsreaduser', 'lmsreaduser', 'iLMSDEV_REPL', 'local replica'
UNION ALL
SELECT true, 'potter', 'mysql', 'local', '10.200.0.42', 3306, 'potter', 'potter', 'Potter', 'local dev';

INSERT INTO framework.datatypes
SELECT 'bigint','DOUBLE PRECISION','DOUBLE PRECISION'
UNION ALL
SELECT 'binary','TEXT','TEXT'
UNION ALL
SELECT 'bit','bit','BOOL'
UNION ALL
SELECT 'char','character varying','TEXT'
UNION ALL
SELECT 'date','date','DATE'
UNION ALL
SELECT 'datetime','datetime','TIMESTAMP WITHOUT TIME ZONE'
UNION ALL
SELECT 'timestamp','timestamp','TIMESTAMP WITHOUT TIME ZONE'
UNION ALL
SELECT 'decimal','decimal','FLOAT'
UNION ALL
SELECT 'float','float','FLOAT'
UNION ALL
SELECT 'image','image','TEXT'
UNION ALL
SELECT 'tinyint','tinyint','FLOAT'
UNION ALL
SELECT 'int','int','FLOAT'
UNION ALL
SELECT 'nchar','char','TEXT'
UNION ALL
SELECT 'ntext','','TEXT'
UNION ALL
SELECT 'numeric','mediumint','FLOAT'
UNION ALL
SELECT 'nvarchar','','TEXT'
UNION ALL
SELECT 'real','','FLOAT'
UNION ALL
SELECT 'smallint','smallint','FLOAT'
UNION ALL
SELECT 'text','text','TEXT'
UNION ALL
SELECT 'tinyint','','FLOAT'
UNION ALL
SELECT 'uniqueidentifier','','TEXT'
UNION ALL
SELECT 'varbinary','','TEXT'
UNION ALL
SELECT 'varchar','varchar','TEXT'
UNION ALL
SELECT 'xml','','TEXT';

INSERT INTO framework.collectionmaps(isactive,instancetype,collection,stage_table,stage_fields,pki_table,pki_fields,app)
SELECT true,'mssql','tbl_user_master','tbl_user_master','*','tbl_user_master_pki','flduserid','lms'
UNION ALL
SELECT true,'mssql','tbl_customer_lookup','tbl_customer_lookup','*','tbl_customer_lookup_pki','fldcustid','lms'
UNION ALL
SELECT true,'mysql','emailcamps','emailcamps','*','emailcamps_pki','','phishproof'
UNION ALL
SELECT true,'mssql','tbl_course_name','tbl_course_name','*','tbl_course_name_pki','flduserdefinedcourseid','lms'
UNION ALL
SELECT true,'mssql','tbl_customer_course','tbl_customer_course','*','tbl_customer_course_pki','intcustid,struserdefinedcourseid','lms'
UNION ALL
SELECT true,'mssql','tbl_student_lookup','tbl_student_lookup','*','tbl_student_lookup_pki','flduserid,fldcustid','lms'
UNION ALL
SELECT true,'mssql','tbl_rulegroup_user','tbl_rulegroup_user','*','tbl_rulegroup_user_pki','intrulegroupid','lms'
UNION ALL
SELECT true,'mssql','tbl_assign_course_student','tbl_assign_course_student','*','tbl_assign_course_student_pki','fldassigncourseid,flduseridassignedto,struserdefinedcourseid','lms'
UNION ALL
SELECT true,'mssql','tbl_department_lookup','tbl_department_lookup','*','tbl_department_lookup_pki','flddeptid','lms'
UNION ALL
SELECT true,'mssql','tbl_division_lookup','tbl_division_lookup','*','tbl_division_lookup_pki','flddivisionid','lms'
UNION ALL
SELECT true,'mssql','tbl_region_lookup','tbl_region_lookup','*','tbl_region_lookup_pki','fldregionid','lms'
UNION ALL
SELECT true,'mssql','tbl_rulegroup_master','tbl_rulegroup_master','*','tbl_rulegroup_master_pki','intrulegroupid','lms'
UNION ALL
SELECT true,'mysql','tbl_salesforce_request_master','tbl_salesforce_request_master','*','tbl_salesforce_request_master_pki','intsalesforcerequestid','potter'
UNION ALL
SELECT true,'mysql','tbl_potter_client_master','tbl_potter_client_master','*','tbl_potter_client_master_pki','intpotterclientid','potter';

UPDATE framework.collectionmaps SET stage_fields= 'fldUserDefinedCourseId,fldCourseName,fldUsageLimitation,fldSendReminder1,fldSendReminder2,fldReminderDays1,fldReminderDays2,fldCourseDuration,fldSetLicense,fldDeliveryMethod,charCourseType,fldDisplayCourseid,fldOwnerCustId,intLicensePeriod,charCourseStatus,charAllowEnrollCourse,convert(VARCHAR,strPrerequisitesUserDefinedCourseIds) as strprerequisitesuserdefinedcourseids ,strCourseCurriculumCategory,intGroupId,bitSendCertificateOnCourseCompletion,strSignatureFilePath,strLeftSignatureFilePath,strHeaderSignatureFilePath,strFooterSignatureFilePath,charDisplaySurvey,charIsSurveyMandatory,charIsVersioningOff,charIsMultiModuleICCourse,charIsCertificationProgramEnable,decPrice,charLicensePeriodType,bitIsEnableLicensePeriod,intCertificationValidPeriod,charCertificationCriteriaField,charIsSendReminderAndReEnrollInCertification,intFirstCertificationReminder,charReEnrollInCertification,charIsSecondCertificationReminderEnable,intSecondCertificationReminder,charIsThirdCertificationReminderEnable,intThirdCertificationReminder,charIsGracePeriodEnable,intGracePeriodDays,charIsSendGracePeriodStartNotification,charSendMessageToUser,charSendMessageToManager,charSendMessageToOrgAdministrator,CharIsSendMessageToOtherEnable,charSendMessageToOther,strUserDefinedCourseIDToEnrollAfterExpiration,charAllowCourseAccessAfterExpirationAndGracePeriod,bitIsLockEnabled,dtmCourseDurationSpecific,bitPastDueSendEmailUsr,bitPastDueLockCourse,bitPastDueSendEmailMgr,charPastDueChangeStatus,IsScormEngineCourse,charSendExpiredMessage,bitSendCertificateHeaderText,bitSendCertificateFooterText,bitEnablePastDueChangeStatus,charIsSendExpiredMessage,charIsSkipCourseStatusScreen,EmbedCount,DisplayCertificateOnUserTranscript,strCourseDirectoryName,charWindowSize,intWindowWidth,intWindowHeight,bitPreventWindowResize,bitIsMEEnable,bitIsCertificationEnable,bitIsAssignedViaOtherEnrollment,IsIELSCORMCourse,bitIfSAChangeLicense,bitIsHistorical,bitIsEnableVolumeDiscount,bitIsSendLicenseExpirationMail,intDaysBeforeLicenseExpiration,tinyintIEngineVersion,dtmCreatedDate,bitIsNew,chariEngineCourseType,bitIsLogoBranding,bitIsPolicyBranding,charDueDateUserProfileField,strCopiedFromUserDefinedCourseID,charCourseImportType,bitIsMobileEnabled,bitIsMobileReady,bitIsMobileOffline,bitIsCyQ'
WHERE collection='tbl_course_name' AND instancetype='mssql';

INSERT INTO framework.jobs(jobid,status,jobname,joborder,biz_starttime,biz_endtime,latency_bizhrs,latency_nonbizhrs,launcher)
SELECT 'popStagingTables.py',true,'Staging Tables',1,'7:00:00'::TIME,'21:00:00'::TIME,600,3600,'python'
UNION ALL
SELECT 'stagePKIFields.py',true,'Get Primary Key Index',3,'7:00:00'::TIME,'21:00:00'::TIME,600,3600,'python'
UNION ALL
SELECT '../IEL_BI/job_Load_Staging_to_DV/job_Load_Staging_to_DV_run.sh',false,'Build Vault',4,'18:00:00'::TIME,'21:00:00'::TIME,600,3600,'sh'
UNION ALL
SELECT '../IEL_BI/job_Drop_Staging_Tables/job_Drop_Staging_Tables_run.sh',false,'PURGE STAGING',5,'18:00:00'::TIME,'21:00:00'::TIME,600,3600,'sh'
UNION ALL
SELECT 'purgeRedundantData.py',true,'Purge duplicate data',2,'7:00:00'::TIME,'21:00:00'::TIME,600,3600,'python';
