import os
import sys
import json
from glob import iglob
from zipfile import ZipFile
from collections import OrderedDict as odict

from dimlib import yml_safe_load, timetracer


@timetracer
def file_scanner(folder):
    return list(
        iglob(
            f'{folder}/*.zip',
            recursive=True
        )
    )


@timetracer
def reporting_dtypes():
    dtypefiles = {
        'mssql': 'dtype_mssql.json',
        'mysql': 'dtype_mysql.json',
    }
    dtypes = odict()
    for _db, _dfile in dtypefiles.items():
        with open(_dfile, 'r') as djson:
            json_txt = djson.read()
        bad_chars = ['\r', '\t', '\n']
        for _char in bad_chars:
            json_txt = json_txt.replace(_char, '')
        dtypes[_db] = yml_safe_load(json_txt.lower())
    return dtypes


def get_csv_structure(zipset, datfile):
    with ZipFile(zipset, 'r') as zet:
        with zet.open(datfile, 'r') as dfile:
            head = dfile.readline()
    header_line = head.decode()
    bad_chars = ['\r', '\t', '\n']
    for _char in bad_chars:
        header_line = header_line.replace(_char, '')
    return ','.join(header_line.lower().split('|'))


def fmt_to_json(
    zipset,
    jsonfile,
    dtdct,
    col_name='column_name',
    col_type='column_type',
):
    pyjson = {}
    zipset_path_split_to_list = zipset.split('/')
    zipset_file = zipset_path_split_to_list[len(zipset_path_split_to_list) - 1]
    if 'lms' in zipset_file:
        dct = dtdct['mssql']
    else:
        dct = dtdct['mysql']
    with ZipFile(zipset, 'r') as zet:
        with zet.open(jsonfile, 'r') as jfile:
            jsonb = jfile.read()
    bad_chars = ['\r', '\t', '\n']
    if isinstance(jsonb, bytes):
        json_txt = jsonb.decode()
    else:
        sys.exit('Unrecognized JSON format')
    for _char in bad_chars:
        json_txt = json_txt.replace(_char, '')
    raw_json = yml_safe_load(json_txt.lower())
    for _r in raw_json:
        if _r[col_type] in dct:
            pyjson[_r[col_name]] = dct[_r[col_type]]
        else:
            pyjson[_r[col_name]] = _r[col_type]
    return {k: v for k, v in pyjson.items() if v not in ['binary']}


@timetracer
def build_file_set(
    all_cfg,
    worker_file,
    active_collections,
):
    xport_cfg = all_cfg['xport_cfg']
    fmt_xtn = xport_cfg['fmt_extension']
    dat_xtn = xport_cfg['dat_extension']
    worker_file_splitter = worker_file.split('/')
    if '' in worker_file_splitter:
        worker_file_splitter.remove('')
    elif ' ' in worker_file_splitter:
        worker_file_splitter.remove(' ')
    version = (worker_file_splitter[-1:][0]).split('.')[0]
    version = version.split('_')[1]
    with ZipFile(worker_file, 'r') as zfile:
        _all_in_zipset = zfile.namelist()
    fmt_files_in_zipset = []
    for _ in _all_in_zipset:
        try:
            if _.split('.')[1] == fmt_xtn:
                fmt_files_in_zipset.append(_)
        except IndexError:
            continue
    file_set = []
    for _file in fmt_files_in_zipset:
        _app_tbl_name = str(os.path.splitext(
            os.path.basename(_file)
        )[0]).lower()
        if _app_tbl_name not in active_collections:
            continue

        dt_ingest_info = {
            'version': version,
            'dataset': worker_file,
            'dat_file': f'{_app_tbl_name}.{dat_xtn}',
            'fmt_file': f'{_app_tbl_name}.{fmt_xtn}',
            'mother_tbl': _app_tbl_name,
            'tbl_name': _app_tbl_name,
            'ins_tbl': f'{version}_{_app_tbl_name}',
        }

        file_set.append(dt_ingest_info)
    return file_set
