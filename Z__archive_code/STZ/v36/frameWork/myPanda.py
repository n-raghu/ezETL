from dtsLib import *

konStr='DRIVER={DTS};SERVER=E-STAGE-DB.inspire.com;DATABASE=ilmsdb;UID=ilmsuser;PWD=ilmsuser'
sqlQuery=	''' SELECT M.fldUserId ,C.fldCustId ,C.fldCustomerName ,M.fldEmployeeId ,M.fldFirstName ,M.fldLastName ,M.fldMiddleName
			,M.fldCity ,M.fldState ,M.fldStateOther ,M.fldCountry ,M.fldAddress1 ,M.fldAddress2 ,M.fldZip ,M.fldEmail
			,M.fldPhone ,M.fldFax ,M.dtmHireDate ,M.strCompanyOrEmployer ,M.fldSupervisor ,M.fldSupervisorEmail ,M.strF024
			,M.strF025 ,M.strF026 ,M.strF027 ,TSL.fldStudentFlag ,TSL.fldRegionId ,TSL.fldDeptId ,TSL.fldDivisionId
			,TRL.fldRegionName ,TDL.fldDeptName ,TVL.fldDivisionName ,TRU.intRuleGroupId ,TRM.strRuleGroupName ,TRU.intRuleGroupUserId
    FROM TBL_USER_MASTER M
    JOIN TBL_STUDENT_LOOKUP TSL ON TSL.fldUserId = M.fldUserId
    JOIN TBL_CUSTOMER_LOOKUP C ON C.fldCustId = TSL.fldCustId
    JOIN TBL_REGION_LOOKUP TRL ON TSL.fldRegionId = TRL.fldRegionId
    JOIN TBL_DEPARTMENT_LOOKUP TDL ON TSL.fldDeptId = TDL.fldDeptId
    JOIN TBL_DIVISION_LOOKUP TVL ON TSL.fldDivisionId = TVL.fldDivisionId
    LEFT JOIN TBL_RULEGROUP_USER TRU ON M.fldUserId = TRU.intUserId
    LEFT JOIN TBL_RULEGROUP_MASTER TRM ON TRU.intRuleGroupId = TRM.intRuleGroupId
        WHERE C.bitIsEAEIntegration=1 AND C.charAccountType='P' AND C.fldCustomerFlag ='A' '''

kon=podbc.connect(konStr)

def buildFrame():
	tt=time()
	pdf=pan.read_sql(sqlQuery,kon)
	tt=time()-tt
	print(tt)
	return pdf
