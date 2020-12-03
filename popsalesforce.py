import sys
import json
from time import time as ttime
from gc import collect as gc_purge
from datetime import datetime as dtm, timedelta as tdt
from collections import OrderedDict as odict, Iterable as cIterable

import requests as req
from sqlalchemy import create_engine as dbeng
from pandas import read_sql_query as rsq, DataFrame as pdf

from dimlib import refresh_config, pgconnector, sql_query_cleanser
from dbops import record_error

if len(sys.argv) < 2:
    pid = int(ttime())
    print(f'PID - {pid} generated for this job.')
else:
    try:
        pid = int(sys.argv[1])
    except ValueError as err:
        sys.exit(err)

gc_purge()
app = 'Salesforce'
row_timestamp = dtm.utcnow()

try:
    cfg = refresh_config()
    dburi = cfg['dburi']
    eaeSchema = cfg['db_schema']
except Exception as err:
    sys.exit(f'Config/INIT Errors|{err}')

pgx = dbeng(dburi)
cnx = pgconnector(dburi)

try:
    with cnx.cursor() as dbcur:
        dbcur.execute('SELECT * FROM framework.api_salesforce_mappers LIMIT 1')
        _ = dbcur.fetchall()
except Exception as err:
    sys.exit(err)

try:
    ndays = cfg['salesforce']['days_frequency']
    TokenPoint = cfg['salesforce']['token']
    tokenParams = {
        'grant_type': 'password',
        'client_id': cfg['salesforce']['cid'],
        'client_secret': cfg['salesforce']['secrect'],
        'username': cfg['salesforce']['uid'],
        'password': cfg['salesforce']['pwd']
    }
    _fmt = cfg['salesforce']['dateformat']
    default_start_date = str(cfg['salesforce']['start_date'])
    tokenHeadR = {
        'content-type': 'application/x-www-form-urlencoded'
    }
    r = req.post(
        TokenPoint,
        params=tokenParams,
        headers=tokenHeadR,
    )
    _record = [
        {
            'app': app,
            'endpoint': r.url,
            'headers': json.dumps(tokenHeadR),
            'params': json.dumps(tokenParams),
            'responsecode': f'{r}',
            'responseok': r.ok,
            'requesttime': dtm.utcnow()
        }
    ]
    if not r.ok:
        record_error(
            cnx,
            pid,
            dtask=app,
            err_src='TokenPoint',
            err_txt=f'{r.text}'
        )
    pdf(_record).to_sql(
        'api_traces',
        pgx,
        index=False,
        if_exists='append',
        schema='framework',
    )
    dataHeadR = {
        'accept': cfg['salesforce']['data_ctype'],
        'content-type': cfg['salesforce']['data_ctype'],
        'Authorization': 'Bearer {}'.format(json.loads(r.text)['access_token'])
    }
except Exception as err:
    record_error(
        cnx,
        pid,
        dtask=app,
        err_src='HeaderPoint',
        err_txt=f'{err}'
    )
    sys.exit(err)

try:
    sql_qry = 'SELECT * FROM framework.api_salesforce_mappers WHERE active=true'
    mapper = rsq(sql_qry, pgx)
    sfdc_collections = list(set(list(mapper['sfdc_collection'])))
    tab_shape = mapper[['sfdc_column', 'sfdc_collection', 'collection']].copy()
    tab_shape['columns'] = tab_shape.groupby(['collection', 'sfdc_collection'])['sfdc_column'].transform(lambda x:','.join(x))
    del tab_shape['sfdc_column']
    tab_shape.drop_duplicates(inplace=True)
    utnow = dtm.utcnow()
except Exception as err:
    record_error(
        cnx,
        pid,
        dtask=app,
        err_src='MappingPoint',
        err_txt=f'{err}'
    )
    sys.exit(err)


def pushChunk(
    pid,
    uri,
    api_frame,
    sfdc_collection,
    dat_collection,
    col_columns,
    start_period,
    end_period=dtm.utcnow()
):
    chunk_status = True
    try:
        pg_cnx = dbeng(uri)
        tracker = pdf(
            {
                'pid': [pid],
                'sfdc_collection': [sfdc_collection],
                'req_start_time': [start_period],
                'req_end_time': [end_period]
            }
        )
        if len(api_frame) > 0:
            del api_frame['attributes']
            api_frame.columns = map(str.lower, api_frame.columns)
            api_frame['row_timestamp'] = row_timestamp
            columnList = col_columns.lower().split(',')
            columnList.append('row_timestamp')
            api_frame[columnList].to_sql(
                dat_collection,
                pg_cnx,
                index=False,
                if_exists='append',
                schema=eaeSchema,
            )

        tracker['req_status'] = True
        tracker.to_sql(
            'api_salesforce_tracker',
            pg_cnx,
            index=False,
            if_exists='append',
            schema='framework',
        )
    except Exception as err:
        chunk_status = False
        record_error(
            cnx,
            pid,
            dtask=app,
            err_src='PushChunkError',
            err_txt=f'{err}'
        )
        tracker['status'] = False
        tracker.to_sql(
            'api_salesforce_tracker',
            pg_cnx,
            index=False,
            if_exists='append',
            schema='framework',
        )
        print(tracker)
    finally:
        pg_cnx.dispose()
    return chunk_status


for _col_ in sfdc_collections:
    try:
        api_query_link = 'v45'
        columns_list = list(mapper.loc[(mapper['sfdc_collection']==_col_),['sfdc_column']]['sfdc_column'])
        column_str = ','.join(columns_list)
        db_collection = list(mapper.loc[(mapper['sfdc_collection']==_col_),['collection']]['collection'])[0]
        api_domain_url = list(mapper.loc[(mapper['sfdc_collection']==_col_),['sfdc_domain_url']]['sfdc_domain_url'])[0]
        api_data_point = list(mapper.loc[(mapper['sfdc_collection']==_col_),['sfdc_data_point']]['sfdc_data_point'])[0]
        api_version_link = api_domain_url + api_data_point
    except Exception as err:
        record_error(
            cnx,
            pid,
            dtask=app,
            err_src='FetchCollectionParams',
            err_txt=f'{err}'
        )
        continue

    while True:
        try:
            break_itr = False
            _col_max_q = f'''SELECT
            COALESCE(MAX(req_end_time),'{default_start_date}') AS col_max_val
                                FROM framework.api_salesforce_tracker
                                WHERE req_status=true
                                AND sfdc_collection='{_col_}';'''
            _col_max_q = sql_query_cleanser(_col_max_q)
            with cnx.cursor() as dbcur:
                dbcur.execute(_col_max_q)
                sql_dat = dbcur.fetchall()
            col_start_date = list(sql_dat)[0][0]
            if (utnow - col_start_date).days > ndays:
                col_max_date = col_start_date + tdt(days=ndays)
                whereClause = f"+where+LastModifiedDate>={col_start_date.isoformat()}Z+and+LastModifiedDate<={col_max_date.isoformat()}Z"
            else:
                break_itr = True
                col_start_date -= tdt(minutes=747)
                whereClause = f'+where+LastModifiedDate>{col_start_date.isoformat()}Z'
            filters = list(mapper.loc[(mapper['sfdc_collection']==_col_),['sfdc_filters']]['sfdc_filters'])[0]
            if filters:
                filters = filters.replace(' ', '+')
                whereClause = f'{whereClause}+and+{filters}'
            api_query_link = api_version_link + '/?q=select+' + column_str + '+from+' +_col_+ whereClause
            R = req.get(api_query_link, headers=dataHeadR)
            _record = [
                {
                    'pid': pid,
                    'app': app,
                    'endpoint': api_query_link,
                    'headers': json.dumps(dataHeadR),
                    'params': None,
                    'responsecode': str(R),
                    'responseok': R.ok,
                    'requesttime': dtm.utcnow()
                }
            ]
            if not R.ok:
                record_error(
                    cnx,
                    pid,
                    dtask=app,
                    err_src=f'SalesforceRequestError|{api_query_link}',
                    err_txt=f'{R.text}'
                )
                break
            pdf(_record).to_sql(
                'api_traces',
                pgx,
                index=False,
                if_exists='append',
                schema='framework'
            )
            api_dat = pdf(json.loads(R.text)['records'])
            while True:
                R_txt = json.loads(R.text)
                if 'nextRecordsUrl' in R_txt:
                    R = req.get(
                        api_domain_url+str(R_txt['nextRecordsUrl']),
                        headers=dataHeadR,
                    )
                    if not R.ok:
                        record_error(
                            cnx,
                            pid,
                            dtask=app,
                            err_src=f'SalesforceExtraRequestError|{api_query_link}',
                            err_txt=f'{R.text}'
                        )
                    xtra_frame = pdf(json.loads(R.text)['records'])
                    api_dat = api_dat.append(
                        xtra_frame.copy(deep=True),
                        sort=False,
                        ignore_index=True,
                    )
                else:
                    break
            if break_itr:
                last_chunk_not_reqd_xclusive_break = pushChunk(
                    pid,
                    uri=dburi,
                    api_frame=api_dat.copy(deep=True),
                    sfdc_collection=_col_,
                    dat_collection=db_collection,
                    col_columns=column_str,
                    start_period=col_start_date
                )
                break
            else:
                res_chk = pushChunk(
                    pid,
                    uri=dburi,
                    api_frame=api_dat.copy(deep=True),
                    sfdc_collection=_col_,
                    dat_collection=db_collection,
                    col_columns=column_str,
                    start_period=col_start_date,
                    end_period=col_max_date,
                )
                if not res_chk:
                    break
        except Exception as err:
            record_error(
                cnx,
                pid,
                dtask=app,
                err_src=f'PrimeException|{_col_}',
                err_txt=f'{err}'
            )
            break

pgx.dispose()
cnx.close()
gc_purge()
