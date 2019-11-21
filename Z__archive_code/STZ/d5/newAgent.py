import dtsKonnections as dct
from dtsLib import arrow,sqlQuery,odict,PDT,Series,dtnow

print('Enter Customer base :')
cList=int(input())
pTime=dtnow.now()
connexions=dct.appFuse()
eaedw=dct.eaeFuse()
kon=connexions.lmsFuse()[1]

def reserve():
    rList=['dtsLib','dtsKonnections','dct','arrow','sqlQuery','odict','PDT','Series','dtnow','defaultTuple','qry','cList','bizLogic'
        ,'practicals','dFrame','iNull','eaedw','pTime']
    return list(set(rList))

def defaultTuple():
    qry={'flduserid':0,'fldcustid':0,'fldcustomername':'dummy','fldfirstname':'dummy'
            ,'fldcity':'dummy','fldstate':'dummy','fldcountry':'dummy','fldaddress1':'dummy','fldzip':'dummy'
            ,'fldemail':'dummy','dtmhiredate':dtnow.utcnow(),
            ,'fldsupervisor':'dummy','fldsupervisoremail':'dummy','fldstudentflag':'dummy','fldregionid':0,'flddeptid':0,
            ,'fldregionname':'dummy','flddeptname':'dummy','intrulegroupid':0,'strrulegroupname':'dummy'}
    return qry

def bizLogic():
    cusOne=int(cList/2)
    cus=[cusOne,int(cList-cusOne)]
    qry='''	SELECT M.fldUserId ,C.fldCustId ,C.fldCustomerName ,M.fldFirstName
			,M.fldCity ,M.fldState ,M.fldCountry ,M.fldAddress1 ,M.fldZip ,M.fldEmail
		    ,M.dtmHireDate ,M.fldSupervisor ,M.fldSupervisorEmail ,TSL.fldStudentFlag 
            ,TSL.fldRegionId ,TSL.fldDeptId
			,TRL.fldRegionName ,TDL.fldDeptName ,TRU.intRuleGroupId ,TRM.strRuleGroupName
    FROM TBL_USER_MASTER M
    JOIN TBL_STUDENT_LOOKUP TSL ON TSL.fldUserId = M.fldUserId
    JOIN TBL_CUSTOMER_LOOKUP C ON C.fldCustId = TSL.fldCustId
    JOIN (SELECT TOP (?+?) FLDCUSTID FROM TBL_CUSTOMER_LOOKUP WHERE bitIsEAEIntegration=1 AND charAccountType='P' AND fldCustomerFlag ='A')TCL ON TCL.fldCustId=C.fldCustId
    JOIN TBL_REGION_LOOKUP TRL ON TSL.fldRegionId = TRL.fldRegionId
    JOIN TBL_DEPARTMENT_LOOKUP TDL ON TSL.fldDeptId = TDL.fldDeptId
    JOIN TBL_DIVISION_LOOKUP TVL ON TSL.fldDivisionId = TVL.fldDivisionId
    LEFT JOIN TBL_RULEGROUP_USER TRU ON M.fldUserId = TRU.intUserId
    LEFT JOIN TBL_RULEGROUP_MASTER TRM ON TRU.intRuleGroupId = TRM.intRuleGroupId '''
    return qry,cus

def practicals():
    print(len(dFrame.index))
    print(len(dFrame[dFrame.dtmhiredate.isnull()]))
    print(len(dFrame[dFrame.dtmhiredate.notnull()]))

    iNull=dFrame.isnull().any()
    print(iNull)
    iNull=dFrame.columns[dFrame.isnull().any()].tolist()
    print(iNull)

    dFrame=dFrame.fillna(value=defaultTuple)
    eaedw.phiCampStats.drop()
    print(len(dFrame[dFrame.dtmhiredate.isnull()]))
    print(len(dFrame[dFrame.dtmhiredate.notnull()]))
    eaedw.phiCampStats.insert_many(dFrame.to_dict('records'))
    return 'X'

qry,cList=bizLogic()
dFrame=sqlQuery(qry,kon['cnx'],params=cList)
dFrame['dtmhiredate']=PDT(Series(dFrame['dtmhiredate']))

print(str(sum(cList))+ ' CUSTOMERS FRAMED IN ' +str(dtnow.now()-pTime))

'''
if(len(frame)>0):
	eaedw.phiCampStats.insert_many(frame)
'''