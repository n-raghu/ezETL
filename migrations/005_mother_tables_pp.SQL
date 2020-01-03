INSERT INTO framework.collectionmaps(collection_name, fld_name, fld_type, src_app)
SELECT 'campaigniplist', 'instancecode', 'TEXT', 'phishproof'
UNION ALL
SELECT 'campaigniplist', 'row_timestamp', 'TIMESTAMP WITHOUT TIME ZONE', 'phishproof'
UNION ALL
SELECT 'campaigniplist', 'rower', 'BIGINT', 'phishproof'
UNION ALL
SELECT 'campaigns', 'instancecode', 'TEXT', 'phishproof'
UNION ALL
SELECT 'campaigns', 'row_timestamp', 'TIMESTAMP WITHOUT TIME ZONE', 'phishproof'
UNION ALL
SELECT 'campaigns', 'rower', 'BIGINT', 'phishproof'
UNION ALL
SELECT 'emailcampaigns', 'instancecode', 'TEXT', 'phishproof'
UNION ALL
SELECT 'emailcampaigns', 'row_timestamp', 'TIMESTAMP WITHOUT TIME ZONE', 'phishproof'
UNION ALL
SELECT 'emailcampaigns', 'rower', 'BIGINT', 'phishproof'
UNION ALL
SELECT 'templates', 'instancecode', 'TEXT', 'phishproof'
UNION ALL
SELECT 'templates', 'row_timestamp', 'TIMESTAMP WITHOUT TIME ZONE', 'phishproof'
UNION ALL
SELECT 'templates', 'rower', 'BIGINT', 'phishproof'
UNION ALL
SELECT 'usertemplates', 'instancecode', 'TEXT', 'phishproof'
UNION ALL
SELECT 'usertemplates', 'row_timestamp', 'TIMESTAMP WITHOUT TIME ZONE', 'phishproof'
UNION ALL
SELECT 'usertemplates', 'rower', 'BIGINT', 'phishproof';