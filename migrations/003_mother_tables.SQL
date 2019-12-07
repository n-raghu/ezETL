INSERT INTO framework.mother_tbl_schema(app, collection_name, active, fld_name, fld_type)
SELECT 'lms', 'tbl_user_master', true, 'instancecode', 'TEXT'
UNION ALL
SELECT 'lms', 'tbl_user_master', true, 'row_timestamp', 'TIMESTAMP WITHOUT TIME ZONE'
UNION ALL
SELECT 'lms', 'tbl_user_master', true, 'flduserid', 'INT'
UNION ALL
SELECT 'lms', 'tbl_assign_course_student', true, 'instancecode', 'TEXT'
UNION ALL
SELECT 'lms', 'tbl_assign_course_student', true, 'row_timestamp', 'TIMESTAMP WITHOUT TIME ZONE'
UNION ALL
SELECT 'lms', 'tbl_assign_course_student', true, 'rower', 'BIGINT';
