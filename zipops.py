from dimlib import os, sys, odict, iglob, ZipFile
from dimlib import yml_safe_load
from dimlib import file_path_splitter
from dimtraces import error_trace, dimlogger


def get_csv_structure(xfile):
    with open(xfile, 'r') as file_itr:
        head = file_itr.readline()
    return ','.join(head.split('|'))


def fmt_to_json(jsonfile):
    with open(jsonfile, 'r') as jfile:
        return yml_safe_load(jfile)


def build_file_set(all_cfg):
    xport_cfg = all_cfg['xport_cfg']
    db_schema = all_cfg['db_schema']
    del all_cfg
    file_path = xport_cfg['archive_path']
    zip_xtn = xport_cfg['archive_extension']
    fmt_xtn = xport_cfg['fmt_extension']
    dat_xtn = xport_cfg['dat_extension']
    zip_set = odict()
    file_set = []
    all_zip_files = list(
        iglob(
            f'{file_path}**/*.{zip_xtn}',
            recursive=True
        )
    )
    for zip_file in all_zip_files:
        _ins_n_ds = file_path_splitter(zip_file, file_path).split('/')
        ins_code = _ins_n_ds[0]
        dataset = _ins_n_ds[1]
        _time_n_app = dataset.split('_')
        timestampid = int(_time_n_app[0])
        app_n_extn = _time_n_app[1]
        _app_n_extn = os.path.splitext(os.path.basename(app_n_extn))
        app_code = _app_n_extn[0]
        if ins_code not in zip_set:
            zip_set[ins_code] = []
        _xlist = zip_set[ins_code]
        _xlist.append(
            {
                'stampid': timestampid,
                'app_code': app_code,
                'dataset': zip_file,
            }
        )
        zip_set[ins_code] = _xlist
    for icode, stampset in zip_set.items():
        sorted_stampset = sorted(stampset, key=lambda _: _['stampid'])
        eligible_stampid = sorted_stampset[0]['stampid']
        zipset_of_eligible_stampid = [
            _ for _ in stampset if _['stampid'] == eligible_stampid
        ]
        for zipset in zipset_of_eligible_stampid:
            with ZipFile(zipset['dataset'], 'r') as zfile:
                _all_in_zipset = zfile.namelist()
            fmt_files_in_zipset = [
                _ for _ in _all_in_zipset if _.split('.')[1] == fmt_xtn
            ]
            for _file in fmt_files_in_zipset:
                _app_tbl_name = os.path.splitext(
                    os.path.basename(_file)
                )[0]
                file_set.append(
                    {
                        'icode': icode,
                        'dataset': zipset['dataset'],
                        'dat_file': f'{_app_tbl_name}.{dat_xtn}',
                        'fmt_file': f'{_app_tbl_name}.{fmt_xtn}',
                        'mother_tbl': _app_tbl_name,
                        'tbl_name': _app_tbl_name,
                        'ins_tbl': f'{icode}_{_app_tbl_name}',
                        'stampid': zipset['stampid'],
                        'app_name': zipset['app_code'],
                        'schema_name': db_schema
                    }
                )
    return file_set
