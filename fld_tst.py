import sys
from glob import iglob
from io import StringIO
from datetime import datetime as dtm
from concurrent.futures import ProcessPoolExecutor

from yaml import safe_load as yml_safe_load
from psycopg2 import connect as pgconnector


# S3_PATH = 'dat/'
with open('story.yml') as cfg_obj:
    cfg = yml_safe_load(cfg_obj)

try:
    dat_file_extn = cfg['file_extension']
    _pgauth = f"{cfg['datastore']['uid']}:{cfg['datastore']['pwd']}"
    _pghost = f"{cfg['datastore']['host']}:{cfg['datastore']['port']}"
    _pgdbo = f"{cfg['datastore']['dbn']}"
    pguri = f'postgresql://{_pgauth}@{_pghost}/{_pgdbo}'
    S3_PATH = f'*.{dat_file_extn}'
    del _pgauth
    del _pgdbo
    del _pghost
except Exception as err:
    sys.exit(f'import-config-err: {err}')

print(pguri)
pgx = pgconnector(pguri)
all_files = [
    _ for _ in iglob(S3_PATH, recursive=True)
]
pg_cp_statement = "COPY fld_str FROM STDIN WITH CSV DELIMITER AS '|' "

with pgx.cursor() as pgcur:
    tbl_statement = 'CREATE TABLE IF NOT EXISTS fld_str(str_fld TEXT)'
    pgcur.execute(tbl_statement)
    pgx.commit()

for dat_file in all_files:
    if 'lite' in dat_file:
        continue
    with open(dat_file, 'r') as dat_obj:
        csv_dat = StringIO()
        for _dat in dat_obj:
            csv_dat.write(_dat)
        csv_dat.seek(0)
        print(_dat)
        with pgx.cursor() as pgcur:
            pgcur.copy_expert(sql=pg_cp_statement, file=csv_dat)

pgx.commit()
