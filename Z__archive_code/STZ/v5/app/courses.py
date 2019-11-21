from dtsAdmin import odict

class course():
	def __init__(mem):
		mem.isEae=1
		mem.accType='(P)'
		mem.custFlag='(A)'
		mem.deliveryMethod=('WB', 'wb')
	def obj(mem):
		tab=[
			odict([('tab','TACS'),('strUserDefinedCourseId','userdefinedcourseid'),('fldUserIdAssignedTo','useridassignedto'),('fldStatus','status')]),
			odict([('tab','TCN'),('fldCourseName','coursename')])
			]
		return tab
	joins=''' FROM TBL_ASSIGN_COURSE_STUDENT TACS
		JOIN TBL_COURSE_NAME TCN ON TACS.strUserDefinedCourseId= TCN.fldUserDefinedCourseId
		JOIN TBL_STUDENT_LOOKUP S ON S.fldUserId = TACS.fldUserIdAssignedTo
		JOIN TBL_CUSTOMER_LOOKUP C ON S.fldCustId = C.fldCustId '''
	def terms(mem):
		where=''' WHERE C.bitIsEAEIntegration=''' +mem.isEae+ '''AND C.charAccountType IN ''' +mem.accType+ '''AND C.fldCustomerFlag IN ''' +mem.custFlag+ ''' AND C.fldCustId=? AND TCN.fldDeliveryMethod IN ''' +mem.deliveryMethod
		return where
