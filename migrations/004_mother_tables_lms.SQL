INSERT INTO framework.collectionmaps(collection_name, fld_name, fld_type, src_app)
SELECT 'tbl_user_master', 'instancecode', 'TEXT', 'lms'
UNION ALL
SELECT 'tbl_user_master', 'row_timestamp', 'TIMESTAMP WITHOUT TIME ZONE', 'lms'
UNION ALL
SELECT 'tbl_user_master', 'rower', 'BIGINT', 'lms'
UNION ALL
SELECT 'tbl_course_name', 'instancecode', 'TEXT', 'lms'
UNION ALL
SELECT 'tbl_course_name', 'row_timestamp', 'TIMESTAMP WITHOUT TIME ZONE', 'lms'
UNION ALL
SELECT 'tbl_course_name', 'rower', 'BIGINT', 'lms'
UNION ALL
SELECT 'tbl_student_lookup', 'instancecode', 'TEXT', 'lms'
UNION ALL
SELECT 'tbl_student_lookup', 'row_timestamp', 'TIMESTAMP WITHOUT TIME ZONE', 'lms'
UNION ALL
SELECT 'tbl_student_lookup', 'rower', 'BIGINT', 'lms'
UNION ALL
SELECT 'tbl_rulegroup_master', 'instancecode', 'TEXT', 'lms'
UNION ALL
SELECT 'tbl_rulegroup_master', 'row_timestamp', 'TIMESTAMP WITHOUT TIME ZONE', 'lms'
UNION ALL
SELECT 'tbl_rulegroup_master', 'rower', 'BIGINT', 'lms'
UNION ALL
SELECT 'tbl_rulegroup_user', 'instancecode', 'TEXT', 'lms'
UNION ALL
SELECT 'tbl_rulegroup_user', 'row_timestamp', 'TIMESTAMP WITHOUT TIME ZONE', 'lms'
UNION ALL
SELECT 'tbl_rulegroup_user', 'rower', 'BIGINT', 'lms'
UNION ALL
SELECT 'tbl_timezone', 'instancecode', 'TEXT', 'lms'
UNION ALL
SELECT 'tbl_timezone', 'row_timestamp', 'TIMESTAMP WITHOUT TIME ZONE', 'lms'
UNION ALL
SELECT 'tbl_timezone', 'rower', 'BIGINT', 'lms'
UNION ALL
SELECT 'tbl_assign_course_student', 'instancecode', 'TEXT', 'lms'
UNION ALL
SELECT 'tbl_assign_course_student', 'row_timestamp', 'TIMESTAMP WITHOUT TIME ZONE', 'lms'
UNION ALL
SELECT 'tbl_assign_course_student', 'rower', 'BIGINT', 'lms'
UNION ALL
SELECT 'tbl_customer_lookup', 'instancecode', 'TEXT', 'lms'
UNION ALL
SELECT 'tbl_customer_lookup', 'row_timestamp', 'TIMESTAMP WITHOUT TIME ZONE', 'lms'
UNION ALL
SELECT 'tbl_customer_lookup', 'rower', 'BIGINT', 'lms'
UNION ALL
SELECT 'tbl_customer_course', 'instancecode', 'TEXT', 'lms'
UNION ALL
SELECT 'tbl_customer_course', 'row_timestamp', 'TIMESTAMP WITHOUT TIME ZONE', 'lms'
UNION ALL
SELECT 'tbl_customer_course', 'rower', 'BIGINT', 'lms'
UNION ALL
SELECT 'tbl_department_lookup', 'instancecode', 'TEXT', 'lms'
UNION ALL
SELECT 'tbl_department_lookup', 'row_timestamp', 'TIMESTAMP WITHOUT TIME ZONE', 'lms'
UNION ALL
SELECT 'tbl_department_lookup', 'rower', 'BIGINT', 'lms'
UNION ALL
SELECT 'tbl_region_lookup', 'instancecode', 'TEXT', 'lms'
UNION ALL
SELECT 'tbl_region_lookup', 'row_timestamp', 'TIMESTAMP WITHOUT TIME ZONE', 'lms'
UNION ALL
SELECT 'tbl_region_lookup', 'rower', 'BIGINT', 'lms'
UNION ALL
SELECT 'tbl_division_lookup', 'instancecode', 'TEXT', 'lms'
UNION ALL
SELECT 'tbl_division_lookup', 'row_timestamp', 'TIMESTAMP WITHOUT TIME ZONE', 'lms'
UNION ALL
SELECT 'tbl_division_lookup', 'rower', 'BIGINT', 'lms'
UNION ALL
SELECT 'tbl_language_master', 'instancecode', 'TEXT', 'lms'
UNION ALL
SELECT 'tbl_language_master', 'row_timestamp', 'TIMESTAMP WITHOUT TIME ZONE', 'lms'
UNION ALL
SELECT 'tbl_language_master', 'rower', 'BIGINT', 'lms'
UNION ALL
SELECT 'tbl_user_master_deleted', 'instancecode', 'TEXT', 'lms'
UNION ALL
SELECT 'tbl_user_master_deleted', 'row_timestamp', 'TIMESTAMP WITHOUT TIME ZONE', 'lms'
UNION ALL
SELECT 'tbl_user_master_deleted', 'rower', 'BIGINT', 'lms'
UNION ALL
SELECT 'tbl_learning_path_groups', 'instancecode', 'TEXT', 'lms'
UNION ALL
SELECT 'tbl_learning_path_groups', 'row_timestamp', 'TIMESTAMP WITHOUT TIME ZONE', 'lms'
UNION ALL
SELECT 'tbl_learning_path_groups', 'rower', 'BIGINT', 'lms'
UNION ALL
SELECT 'tbl_learning_path_objects', 'instancecode', 'TEXT', 'lms'
UNION ALL
SELECT 'tbl_learning_path_objects', 'row_timestamp', 'TIMESTAMP WITHOUT TIME ZONE', 'lms'
UNION ALL
SELECT 'tbl_learning_path_objects', 'rower', 'BIGINT', 'lms'
UNION ALL
SELECT 'tbl_learning_path_master', 'instancecode', 'TEXT', 'lms'
UNION ALL
SELECT 'tbl_learning_path_master', 'row_timestamp', 'TIMESTAMP WITHOUT TIME ZONE', 'lms'
UNION ALL
SELECT 'tbl_learning_path_master', 'rower', 'BIGINT', 'lms'
UNION ALL
SELECT 'tbl_learning_path_user', 'instancecode', 'TEXT', 'lms'
UNION ALL
SELECT 'tbl_learning_path_user', 'row_timestamp', 'TIMESTAMP WITHOUT TIME ZONE', 'lms'
UNION ALL
SELECT 'tbl_learning_path_user', 'rower', 'BIGINT', 'lms'
UNION ALL
SELECT 'tbl_pp_drip_campaign_master', 'instancecode', 'TEXT', 'lms'
UNION ALL
SELECT 'tbl_pp_drip_campaign_master', 'row_timestamp', 'TIMESTAMP WITHOUT TIME ZONE', 'lms'
UNION ALL
SELECT 'tbl_pp_drip_campaign_master', 'rower', 'BIGINT', 'lms'
UNION ALL
SELECT 'tbl_lms_scorm_course_metadata', 'instancecode', 'TEXT', 'lms'
UNION ALL
SELECT 'tbl_lms_scorm_course_metadata', 'row_timestamp', 'TIMESTAMP WITHOUT TIME ZONE', 'lms'
UNION ALL
SELECT 'tbl_lms_scorm_course_metadata', 'rower', 'BIGINT', 'lms';
