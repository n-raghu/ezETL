import sys
import random as r
from datetime import datetime as dtm, timedelta as tdt


N = 1
B = 1000000
if len(sys.argv) > 1:
    N = int(sys.argv[1])
    try:
        file_create = sys.argv[2]
    except Exception:
        file_create = False
else:
    file_create = False
file_name = 'user_master.db8io'


def pop_csv_dat(loop):
    csv_dat = []
    bool_val = [True, False]
    for _ in range(loop):
        _rint = r.randint(1, 10000)
        my_f_str = f'{_rint}|txt_{_rint}|swiss|{dtm.now()}|{r.choice(bool_val)}|{dtm.now() - tdt(_rint)}|{r.choice(bool_val)}|{dtm.now() - tdt(_rint)}\n'
        csv_dat.append(my_f_str)
    return csv_dat


if file_create:
    with open(file_name, 'w') as fil_obj:
        fil_obj.write('f_bool|f_int|f_str|f_stamp|f_float')
        fil_obj.write('\n')

for n in range(N):
    with open(file_name, 'a') as fil_obj:
        fil_obj.writelines(pop_csv_dat(B))
        print(f'{n} completed.')
