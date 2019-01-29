debug=False

'''
import sys
if all([len(sys.argv)<2, debug==False]):
	sys.exit('PID not provided... ')
elif debug:
	pid=-1
else:
	pid=int(sys.argv[1])
'''

from dimlib import *

r.init()
uri=uri
csize=cfg['pandas']['chunksize']
tracker=pdf([],columns=['status','instancecode','collection','timestarted','timefinished','rowversion'])
objFrame=[]

def recordRowVersions():
	global tracker
	if tracker.empty:
		print('Tracker is Empty ')
		issue=True
	else:
		pgx=pgcnx(uri)
		tracker['pid']=pid
		tracker['instancetype']='mssql'
		tracker.fillna(-1,inplace=True)
		tracker.to_sql('chunktraces',pgx,if_exists='append',index=False,schema='framework')
		ixx=tracker.groupby(['instancecode','collection'],sort=False)['rowversion'].transform(max)==tracker['rowversion']
		tracker[ixx].to_sql('collectiontracker',pgx,if_exists='append',index=False,schema='framework')
		del tracker
		issue=False
	return issue

mssql_dict=objects_mssql(uri)
insList=mssql_dict['insList']
colFrame=mssql_dict['frame']

#@r.remote
def popCollections(icode,connexion,iFrame):
	pgx=pgcnx(uri)
	sqx=sqlCnx(connexion)
	chunk=pdf([],columns=['model'])
	trk=pdf([],columns=['status','collection','timestarted','timefinished','rowversion'])
	for idx,rowdata in iFrame.iterrows():
		rco=rowdata['collection']
		trk=trk.append({'status':False,'collection':rco,'timestarted':dtm.utcnow()},ignore_index=True)
		sql="SELECT '" +icode+ "' as instancecode,*,CAST(sys_ROWVERSION AS BIGINT) AS ROWER FROM " +rco+ "(NOLOCK) WHERE CAST(sys_ROWVERSION AS BIGINT) > " +str(int(rowdata['rower']))
		sql=''' select  fldUserDefinedCourseId,fldCourseName,fldUsageLimitation,fldSendReminder1,fldSendReminder2,fldReminderDays1,fldReminderDays2,fldCourseDuration,fldSetLicense,fldDeliveryMethod,charCourseType,fldDisplayCourseid,fldOwnerCustId,intLicensePeriod,charCourseStatus,charAllowEnrollCourse,convert(VARCHAR,strPrerequisitesUserDefinedCourseIds),strCourseCurriculumCategory,intGroupId,bitSendCertificateOnCourseCompletion,strSignatureFilePath,strLeftSignatureFilePath,strHeaderSignatureFilePath,strFooterSignatureFilePath,charDisplaySurvey,charIsSurveyMandatory,charIsVersioningOff,charIsMultiModuleICCourse,charIsCertificationProgramEnable,decPrice,charLicensePeriodType,bitIsEnableLicensePeriod,intCertificationValidPeriod,charCertificationCriteriaField,charIsSendReminderAndReEnrollInCertification,intFirstCertificationReminder,charReEnrollInCertification,charIsSecondCertificationReminderEnable,intSecondCertificationReminder,charIsThirdCertificationReminderEnable,intThirdCertificationReminder,charIsGracePeriodEnable,intGracePeriodDays,charIsSendGracePeriodStartNotification,charSendMessageToUser,charSendMessageToManager,charSendMessageToOrgAdministrator,CharIsSendMessageToOtherEnable,charSendMessageToOther,strUserDefinedCourseIDToEnrollAfterExpiration,charAllowCourseAccessAfterExpirationAndGracePeriod,bitIsLockEnabled,dtmCourseDurationSpecific,bitPastDueSendEmailUsr,bitPastDueLockCourse,bitPastDueSendEmailMgr,charPastDueChangeStatus,IsScormEngineCourse,charSendExpiredMessage,bitSendCertificateHeaderText,bitSendCertificateFooterText,bitEnablePastDueChangeStatus,charIsSendExpiredMessage,charIsSkipCourseStatusScreen,EmbedCount,DisplayCertificateOnUserTranscript,strCourseDirectoryName,charWindowSize,intWindowWidth,intWindowHeight,bitPreventWindowResize,bitIsMEEnable,bitIsCertificationEnable,bitIsAssignedViaOtherEnrollment,IsIELSCORMCourse,bitIfSAChangeLicense,bitIsHistorical,bitIsEnableVolumeDiscount,bitIsSendLicenseExpirationMail,intDaysBeforeLicenseExpiration,tinyintIEngineVersion,dtmCreatedDate,bitIsNew,chariEngineCourseType,bitIsLogoBranding,bitIsPolicyBranding,charDueDateUserProfileField,strCopiedFromUserDefinedCourseID,charCourseImportType,bitIsMobileEnabled,bitIsMobileReady,bitIsMobileOffline,bitIsCyQ from tbl_course_name (NOLOCK) '''
		chunk=rsq(sql,sqx)
		print(chunk)
	del chunk
	sqx.close()
	pgx.dispose()
	trk['instancecode']=icode
	return trk

print('Active Instances Found: ' +str(len(insList)))
if all([debug==False,len(insList)>0]):
	for ins in insList:
		cnxStr=ins['sqlConStr']
		instancecode=ins['icode']
		iFrame=colFrame.loc[(colFrame['icode']==instancecode) & (colFrame['instancetype']=='mssql'),['collection','s_table','rower']]
		#objFrame.append(popCollections.remote(instancecode,cnxStr,iFrame))
		popCollections(instancecode,cnxStr,iFrame)
	r.wait(objFrame)
	for obj in objFrame:
		tracker=tracker.append(r.get(obj),sort=False,ignore_index=True)
	del objFrame
	r.shutdown()
	recordRowVersions()
elif debug:
	print('Ready to DEBUG... ')
else:
	print('No Active Instances Found.')
