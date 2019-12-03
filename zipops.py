from dimlib import os, sys
from dimlib import ZipFile, yml_safe_load
from dimlib import error_trace, dimlogger


def get_csv_structure(xfile):
    with open(xfile, 'r') as file_itr:
        head = file_itr.readline()
    return ','.join(head.split('|'))


def fmt_to_json(jsonfile):
    with open(jsonfile, 'r') as jfile:
        return yml_safe_load(jfile)


def get_file_set(file_path):
    file_set = []
    all_files = list(
        iglob(
            f'{file_path}**/*.{dat_file_extn}',
            recursive=True
        )
    )
    print(all_files)
    for dat_file in all_files:
        ins_file_name = file_path_splitter(dat_file, file_path)
        print(ins_file_name)
        _ilist = ins_file_name.split('/')
        ins_code = _ilist[0]
        _flist = dat_file.split('.')
        _file = _flist[0]
        _mother_tbl = os.path.splitext(os.path.basename(dat_file))[0]
        file_set.append(
            {
                'icode': ins_code,
                'dat_file': dat_file,
                'fmt_file': f'{_file}.{fmt_file_extn}',
                'mother_tbl': _mother_tbl,
                'ins_tbl': f'{ins_code}_{_mother_tbl}',
            }
        )
    return file_set
