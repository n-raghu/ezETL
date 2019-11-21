from dtsLib import *

def customers():
  return ''' SELECT fldCustId FROM TBL_CUSTOMER_LOOKUP WHERE bitIsEAEIntegration=1 AND charAccountType='P' AND fldCustomerFlag='A' '''

def studentQuery():
    sql=''' SELECT M.fldUserId[userid],C.fldCustId[custid],C.fldCustomerName[customername],TSL.fldStudentFlag[studentflag]
    ,TSL.fldRegionId[regionid],TSL.fldDeptId[deptid],TSL.fldDivisionId[divisionid],TRL.fldRegionName[regionname],TDL.fldDeptName[deptname]
    ,TVL.fldDivisionName[divisionname],TRU.intRuleGroupId[groupid],TRM.strRuleGroupName[groupname],TRU.intRuleGroupUserId[groupuserid]
        FROM TBL_USER_MASTER M
        JOIN TBL_STUDENT_LOOKUP TSL ON TSL.fldUserId=M.fldUserId
        JOIN TBL_CUSTOMER_LOOKUP C ON C.fldCustId=TSL.fldCustId
        JOIN TBL_REGION_LOOKUP TRL ON TSL.fldRegionId=TRL.fldRegionId
        JOIN TBL_DEPARTMENT_LOOKUP TDL ON TSL.fldDeptId=TDL.fldDeptId
        JOIN TBL_DIVISION_LOOKUP TVL ON TSL.fldDivisionId=TVL.fldDivisionId
        LEFT JOIN TBL_RULEGROUP_USER TRU ON M.fldUserId=TRU.intUserId
        LEFT JOIN TBL_RULEGROUP_MASTER TRM ON TRU.intRuleGroupId=TRM.intRuleGroupId
            WHERE C.bitIsEAEIntegration=1 AND C.charAccountType='P' AND C.fldCustomerFlag ='A' AND C.fldCustId=? '''
    return sql
