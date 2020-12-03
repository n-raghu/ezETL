def faq_phiHetro():
    queries=[]
    queries.append(''' CREATE TABLE IF NOT EXISTS FarmStats_HetroData AS(
SELECT C.ID,C.TemplateID,DATE(C.StartDate) SDATE,DATE(C.EndDate) EDATE,E.IsVictim,DATE(E.ClickDate) CDATE,DATE(E.FormDate) FDATE,DATE(E.AttachmentOpened) ADATE,DATE(E.Opened) ODATE,DATE(E.SendDate) MDATE
FROM campaigns C
JOIN emailcampaigns E ON C.ID=E.CampaignID
WHERE E.EmailSent=1) ''')
    queries.append(''' SELECT WEEK(SDATE,0) WOY,FLOOR((DAYOFMONTH(SDATE) - 1) / 7) + 1 WOM,DATE_FORMAT(SDATE,"%b") M,YEAR(SDate) YYY,DAYOFYEAR(SDate) DOY,TIMESTAMP(SDATE) SDT,COUNT(DISTINCT ID) KNT
FROM FarmStats_HetroData
GROUP BY SDATE ''')
    queries.append(''' SELECT WEEK(SDATE,0) WOY,FLOOR((DAYOFMONTH(SDATE) - 1) / 7) + 1 WOM,DATE_FORMAT(SDATE,"%b") M,YEAR(SDate) YYY,DAYOFYEAR(SDate) DOY,TIMESTAMP(SDATE) SDT,COUNT(DISTINCT TemplateID) KNT
FROM FarmStats_HetroData
GROUP BY SDATE ''')
    queries.append(''' SELECT WEEK(SDATE,0) WOY,FLOOR((DAYOFMONTH(SDATE) - 1) / 7) + 1 WOM,DATE_FORMAT(SDATE,"%b") M,YEAR(SDate) YYY,DAYOFYEAR(SDate) DOY,TIMESTAMP(SDATE) SDT,COUNT(MDATE) KNT
FROM FarmStats_HetroData
WHERE MDATE IS NOT NULL
GROUP BY SDATE ''')
    queries.append(''' SELECT WEEK(SDATE,0) WOY,FLOOR((DAYOFMONTH(SDATE) - 1) / 7) + 1 WOM,DATE_FORMAT(SDATE,"%b") M,YEAR(SDate) YYY,DAYOFYEAR(SDate) DOY,TIMESTAMP(SDATE) SDT,COUNT(IsVictim) KNT
FROM FarmStats_HetroData
WHERE IsVictim IS NOT NULL
GROUP BY SDATE ''')
    queries.append(''' SELECT WEEK(SDATE,0) WOY,FLOOR((DAYOFMONTH(SDATE) - 1) / 7) + 1 WOM,DATE_FORMAT(SDATE,"%b") M,YEAR(SDate) YYY,DAYOFYEAR(SDate) DOY,TIMESTAMP(SDATE) SDT,COUNT(CDATE) KNT
FROM FarmStats_HetroData
WHERE CDATE IS NOT NULL
GROUP BY SDATE ''')
    queries.append(''' SELECT WEEK(SDATE,0) WOY,FLOOR((DAYOFMONTH(SDATE) - 1) / 7) + 1 WOM,DATE_FORMAT(SDATE,"%b") M,YEAR(SDate) YYY,DAYOFYEAR(SDate) DOY,TIMESTAMP(SDATE) SDT,COUNT(FDATE) KNT
FROM FarmStats_HetroData
WHERE FDATE IS NOT NULL
GROUP BY SDATE ''')
    queries.append(''' SELECT WEEK(SDATE,0) WOY,FLOOR((DAYOFMONTH(SDATE) - 1) / 7) + 1 WOM,DATE_FORMAT(SDATE,"%b") M,YEAR(SDate) YYY,DAYOFYEAR(SDate) DOY,TIMESTAMP(SDATE) SDT,COUNT(ODATE) KNT
FROM FarmStats_HetroData
WHERE ODATE IS NOT NULL
GROUP BY SDATE ''')
    queries.append(''' SELECT WEEK(SDATE,0) WOY,FLOOR((DAYOFMONTH(SDATE) - 1) / 7) + 1 WOM,DATE_FORMAT(SDATE,"%b") M,YEAR(SDate) YYY,DAYOFYEAR(SDate) DOY,TIMESTAMP(SDATE) SDT,COUNT(ADATE) KNT
FROM FarmStats_HetroData
WHERE ADATE IS NOT NULL
GROUP BY SDATE ''')
    queries.append(''' DROP TABLE FarmStats_HetroData ''')
    return queries

def faq_lmsCourseCompletion():
    	query=[]
    	query.append(''' IF OBJECT_ID('TEMPDB..##FARMSTATS_CC_',N'U') IS NOT NULL DROP TABLE ##FARMSTATS_CC_ ''')
    	query.append(''' SELECT * INTO ##FARMSTATS_CC_
    	FROM( SELECT S.fldStudentFlag[studentflag],M.fldUserId[userid],C.fldCustId[custid],C.fldCustomerName[customername],M.fldEmail[email],M.fldEmployeeId[employeeid]
    		,TACS.fldStatus[status],TACS.fldUserIdAssignedTo[useridassignedto],CONVERT(DATE,TACS.fldAssignedOnDate)[assigneddate],CONVERT(DATE,TACS.fldCompletedOnDate)[completedondate]
    		,TCN.fldUserDefinedCourseId[userdefinedcourseid],TCN.fldCourseName[coursename]
    	FROM TBL_CUSTOMER_LOOKUP(NOLOCK) C
    	JOIN TBL_USER_MASTER(NOLOCK) M ON C.fldCustId=M.intCustomerID
    	JOIN TBL_STUDENT_LOOKUP(NOLOCK) S ON S.fldCustId=C.fldCustId AND S.fldUserId=M.fldUserId
    	JOIN TBL_ASSIGN_COURSE_STUDENT(NOLOCK) TACS ON M.fldUserId=TACS.fldUserIdAssignedTo
    	JOIN TBL_COURSE_NAME TCN(NOLOCK) ON TCN.fldUserDefinedCourseId=TACS.strUserDefinedCourseId
    		WHERE TCN.IsScormEngineCourse=1 AND C.fldCustomerFlag='A' AND C.charAccountType='P' AND S.fldStudentFlag IN ('A','I','D')
    				AND TCN.tinyintIEngineVersion>3 AND TCN.fldDeliveryMethod='wb' AND TCN.charCourseImportType IN ('S','X'))A ''')
    	query.append(''' SELECT YYY,DOY,CONVERT(DATETIME,assigneddate)[assigneddate],MWEEK,MONTH,knt,woy FROM(SELECT DATEPART(YY,assigneddate)[YYY],DATEPART(DY,assigneddate)[DOY],assigneddate,DATEDIFF(WEEK,DATEADD(MONTH,DATEDIFF(MONTH,0,assigneddate),0),assigneddate)+1 [MWEEK],SUBSTRING(DATENAME(M,assigneddate),1,3)[MONTH],COUNT(userid)[knt],DATEPART(ISOWW,assigneddate)[woy] FROM ##FARMSTATS_CC_ WHERE assigneddate IS NOT NULL GROUP BY assigneddate)A ''')
    	query.append(''' SELECT YYY,DOY,CONVERT(DATETIME,completedondate)[completeddate],MWEEK,MONTH,knt,woy FROM(SELECT DATEPART(YY,completedondate)[YYY],DATEPART(DY,completedondate)[DOY],completedondate,DATEDIFF(WEEK,DATEADD(MONTH,DATEDIFF(MONTH,0,completedondate),0),completedondate)+1 [MWEEK],SUBSTRING(DATENAME(M,completedondate),1,3)[MONTH],COUNT(userid)[knt],DATEPART(ISOWW,completedondate)[woy] FROM ##FARMSTATS_CC_ WHERE completedondate IS NOT NULL GROUP BY completedondate)A ''')
    	query.append(''' DROP TABLE ##FARMSTATS_CC_ ''')
    	return query
