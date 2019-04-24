import requests as req

point='https://cs70.salesforce.com/services/oauth2/token'
inParams={'grant_type':'password'
    ,'client_id':'3MVG9PE4xB9wtoY8w4VZoDIfOOua2XA_iJw9cGi270bVwJAJ6S4iZ5mcpffviN.GJ_Fx0.UF_eimRFRVmRxeL'
    ,'client_secret':'1F35E6ED29FB35F484B562F02236479E92F16B71BE5A5B1CED787AD42EBB7D2D'
    ,'username':'divs@kcloudtechnologies.com.elearning'
    ,'password':'Passion@3owcF8Vw8VpqodZY7v9boU3Ph'
    }
headers={'content-type':'application/x-www-form-urlencoded'}

r=req.post(point,params=inParams,headers=headers)

print(r)
print(r.headers)
print(r.text)
