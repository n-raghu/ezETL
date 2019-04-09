DECLARE @stamp VARCHAR(64)='sys_ROWVERSION', @i INT=1, @tname VARCHAR(128), @sql VARCHAR(8000)
DECLARE @collections TABLE(IDI INT IDENTITY(1,1), TNAME VARCHAR(128))
DECLARE @results TABLE(INF VARCHAR(1000))

INSERT @collections
SELECT 'tbl_Customer_Lookup'
UNION ALL
SELECT 'tbl_User_Master'
UNION ALL
SELECT 'tbl_Student_Lookup'
UNION ALL
SELECT 'tbl_Assign_Course_Student'
UNION ALL
SELECT 'tbl_Course_Name'
UNION ALL
SELECT 'tbl_Customer_Course'
UNION ALL
SELECT 'tbl_RuleGroup_User'
UNION ALL
SELECT 'tbl_Department_Lookup'
UNION ALL
SELECT 'tbl_Division_Lookup'
UNION ALL
SELECT 'tbl_Region_Lookup'
UNION ALL
SELECT 'tbl_rulegroup_master'

WHILE @i<=(SELECT MAX(IDI) FROM @collections)
BEGIN
SELECT @tname=TNAME FROM @collections WHERE IDI=@i
IF COL_LENGTH(@tname,@stamp) IS NULL
BEGIN
SELECT @sql='ALTER TABLE ' +@tname+ ' ADD ' +@stamp+ ' ROWVERSION NOT NULL'
EXEC (@sql)
INSERT @results
SELECT @tname + ' - NEWLY CREATED'
END
ELSE
INSERT @results
SELECT @tname + ' - ALREADY EXISTS'
SELECT @i=@i+1
END

SELECT * FROM @results
