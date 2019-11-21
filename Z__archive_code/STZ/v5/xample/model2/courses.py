select=''' SELECT TACS.strUserDefinedCourseId ,TCN.fldCourseName ,TACS.fldUserIdAssignedTo ,TACS.fldStatus ,TACS.fldPercentage
	,TACS.intTimeSpent ,TACS.fldCompletedOnDate ,TACS.fldAssignCourseId '''
joins=''' FROM TBL_ASSIGN_COURSE_STUDENT TACS
		JOIN TBL_COURSE_NAME TCN ON TACS.strUserDefinedCourseId= TCN.fldUserDefinedCourseId
		JOIN TBL_STUDENT_LOOKUP S ON S.fldUserId = TACS.fldUserIdAssignedTo
		JOIN TBL_CUSTOMER_LOOKUP C ON S.fldCustId = C.fldCustId '''
where=''' WHERE C.bitIsEAEIntegration=1 AND C.charAccountType='P' AND C.fldCustomerFlag ='A'
			AND C.fldCustId=? AND TCN.fldDeliveryMethod IN ('WB', 'wb') '''
cto=''' JOIN CHANGETABLE(CHANGES TBL_COURSE_NAME, 1) CT '''

def course():
	global select,joins,where,cto
	return select,joins,where,cto