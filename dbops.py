from dimlib import os, sys, iglob

def create_mother_tables(pguri):
    cnx = pgconnector(pguri)
    with open('mother_tbl_schema.json', 'r') as jfile:
        schema_set = yml_safe_load(jfile)
    tbl_statements = []
    for tbl in schema_set:
        this_tbl_fields = ''
        for column_name, data_type in tbl.items():
            this_tbl_fields += f'{column_name} {data_type},'
        if this_tbl_fields.endswith(','):
            this_tbl_fields = this_tbl_fields[:-1]
        tbl_statements.append(
            f'CREATE TABLE IF NOT EXISTS user_master({this_tbl_fields})'
        )
    with cnx.cursor() as pgcur:
        for stmt in tbl_statements:
            pgcur.execute(stmt)
    cnx.commit()
    del tbl_statements
    cnx.close()
    return schema_set


def create_ins_tbl(
    pgx,
    mother_tbl,
    ins_tbl,
    mother_tbl_structure,
    ins_tbl_structure
):
    tbl_columns = ''
    ins_only_columns = set(ins_tbl_structure) - set(mother_tbl_structure)
    for fld in ins_only_columns:
        tbl_columns += f'{fld} {ins_tbl_structure[fld]},'
    if tbl_columns.endswith(','):
        tbl_columns = tbl_columns[:-1]
    tbl_statement = f''' CREATE TABLE IF NOT EXISTS {ins_tbl}(
        {tbl_columns}
        ) INHERITS ({mother_tbl});'''
    try:
        with pgx.cursor() as pgcur:
            pgcur.execute(tbl_statement)
            pgx.commit()
        return True
    except Exception as err:
        return err
