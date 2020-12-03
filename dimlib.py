import re
import os
import sys

from yaml import safe_load as yml_safe_load


def refresh_config():
    cfg = {}
    with open('config.yml') as _cfg_obj:
        raw_cfg = yml_safe_load(_cfg_obj)
    try:
        _pgauth = f"{raw_cfg['datastore']['user']}:{raw_cfg['datastore']['passwd']}"
        _pghost = f"{raw_cfg['datastore']['server']}:{raw_cfg['datastore']['port']}"
        _pgdbo = f"{raw_cfg['datastore']['db']}"
        path_zip_dat = f"{raw_cfg['xport_cfg']['zip_path']}"
        if not path_zip_dat.endswith('/'):
            path_zip_dat += '/'
        path_zip_archive = f"{raw_cfg['xport_cfg']['archive_path']}"
        if not path_zip_archive.endswith('/'):
            path_zip_archive += '/'
        cfg = {
            'dburi': f'postgresql://{_pgauth}@{_pghost}/{_pgdbo}',
            'cpu_workers': raw_cfg['max_workers'],
            'xport_cfg': raw_cfg['xport_cfg'],
        }
    except Exception as err:
        sys.exit(f'dimlib|import-config-err|{err}')
    return cfg


def file_path_splitter(dat_file_with_path, file_path):
    index = dat_file_with_path.find(file_path)
    if index != -1 and index + len(file_path) < len(dat_file_with_path):
        return dat_file_with_path[index + len(file_path):]
    else:
        return None


def sql_query_cleanser(sequel_y):
    bad_chars = ['\r', '\t', '\n', ]
    for _char in bad_chars:
        sequel_y = sequel_y.replace(_char, '')
    return re.sub(' +', ' ', sequel_y)


def file_decrypter_buff(
    enc_file,
    passcode,
    gpg_ins,
    unzip_path='_buff',
    zip_xtn='io',
):
    _file_pattern = enc_file.split('/')
    _len = len(_file_pattern) - 1
    _file = _file_pattern[_len]
    buff_path = ''
    for _ in range(_len):
        buff_path = f'{buff_path}{_file_pattern[_]}/'
    buff_path += unzip_path
    _file_pattern = _file.split('.')
    file_name = _file_pattern[0]
    zip_file = f'{buff_path}/{file_name}.{zip_xtn}'
    print(zip_file)
    with open(enc_file, 'rb') as efile:
        _ins = gpg_ins.decrypt_file(
            efile,
            passphrase=passcode,
            output=zip_file,
        )
    return _ins
