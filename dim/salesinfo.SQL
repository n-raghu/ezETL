
DROP TABLE framework.api_mappers;
CREATE TABLE IF NOT EXISTS framework.api_mappers(
active BOOL,
app TEXT,
point_name TEXT,
endpoint TEXT,
point_col TEXT,
api_keycol TEXT,
dtype_collection TEXT,
collection TEXT
);

INSERT INTO framework.api_mappers(active,app,point_name,endpoint,point_col,dtype_collection,collection,api_keycol)
SELECT true,'salesforce','OpportunityTracker','https://cs70.salesforce.com/services/apexrest/getClosedWonOpportunities','AccountId','text','salesforce_accounts','opportunityid'
UNION ALL
SELECT true,'salesforce','OpportunityTracker','https://cs70.salesforce.com/services/apexrest/getClosedWonOpportunities','accountname','text','salesforce_accounts','opportunityid'
UNION ALL
SELECT true,'salesforce','OpportunityTracker','https://cs70.salesforce.com/services/apexrest/getClosedWonOpportunities','AccountExpiryDate','date','salesforce_accounts','opportunityid'
UNION ALL
SELECT true,'salesforce','OpportunityTracker','https://cs70.salesforce.com/services/apexrest/getClosedWonOpportunities','accountcreatedDate','date','salesforce_accounts','opportunityid'
UNION ALL
SELECT true,'salesforce','OpportunityTracker','https://cs70.salesforce.com/services/apexrest/getClosedWonOpportunities','typeOfPartner','text','salesforce_accounts','opportunityid'
UNION ALL
SELECT true,'salesforce','OpportunityTracker','https://cs70.salesforce.com/services/apexrest/getClosedWonOpportunities','Opportunityid','text','salesforce_opportunities','opportunityid'
UNION ALL
SELECT true,'salesforce','OpportunityTracker','https://cs70.salesforce.com/services/apexrest/getClosedWonOpportunities','lineitems.productname','text','salesforce_opportunities','opportunityid'
UNION ALL
SELECT true,'salesforce','OpportunityTracker','https://cs70.salesforce.com/services/apexrest/getClosedWonOpportunities','lineitems.productcode','text','salesforce_opportunities','opportunityid'
UNION ALL
SELECT true,'salesforce','OpportunityTracker','https://cs70.salesforce.com/services/apexrest/getClosedWonOpportunities','lineitems.productid','text','salesforce_opportunities','opportunityid'
