INSERT INTO framework.mother_tbl_schema(app, collection_name, active, fld_name, fld_type)
SELECT 'lms', 'tbl_user_master', true, 'instancecode', 'TEXT'
UNION ALL
SELECT 'lms', 'tbl_user_master', true, 'row_timestamp', 'TIMESTAMP WITHOUT TIME ZONE'
UNION ALL
SELECT 'lms', 'tbl_user_master', true, 'rower', 'BIGINT'
UNION ALL
SELECT 'lms', 'tbl_course_name', true, 'instancecode', 'TEXT'
UNION ALL
SELECT 'lms', 'tbl_course_name', true, 'row_timestamp', 'TIMESTAMP WITHOUT TIME ZONE'
UNION ALL
SELECT 'lms', 'tbl_course_name', true, 'rower', 'BIGINT'
UNION ALL
SELECT 'lms', 'tbl_student_lookup', true, 'instancecode', 'TEXT'
UNION ALL
SELECT 'lms', 'tbl_student_lookup', true, 'row_timestamp', 'TIMESTAMP WITHOUT TIME ZONE'
UNION ALL
SELECT 'lms', 'tbl_student_lookup', true, 'rower', 'BIGINT'
UNION ALL
SELECT 'lms', 'tbl_rulegroup_master', true, 'instancecode', 'TEXT'
UNION ALL
SELECT 'lms', 'tbl_rulegroup_master', true, 'row_timestamp', 'TIMESTAMP WITHOUT TIME ZONE'
UNION ALL
SELECT 'lms', 'tbl_rulegroup_master', true, 'rower', 'BIGINT'
UNION ALL
SELECT 'lms', 'tbl_rulegroup_user', true, 'instancecode', 'TEXT'
UNION ALL
SELECT 'lms', 'tbl_rulegroup_user', true, 'row_timestamp', 'TIMESTAMP WITHOUT TIME ZONE'
UNION ALL
SELECT 'lms', 'tbl_rulegroup_user', true, 'rower', 'BIGINT'
UNION ALL
SELECT 'lms', 'tbl_timezone', true, 'instancecode', 'TEXT'
UNION ALL
SELECT 'lms', 'tbl_timezone', true, 'row_timestamp', 'TIMESTAMP WITHOUT TIME ZONE'
UNION ALL
SELECT 'lms', 'tbl_timezone', true, 'rower', 'BIGINT'
UNION ALL
SELECT 'lms', 'tbl_assign_course_student', true, 'instancecode', 'TEXT'
UNION ALL
SELECT 'lms', 'tbl_assign_course_student', true, 'row_timestamp', 'TIMESTAMP WITHOUT TIME ZONE'
UNION ALL
SELECT 'lms', 'tbl_assign_course_student', true, 'rower', 'BIGINT';
