from functools import wraps
from time import process_time as tpt


def bugtracer():
    pass


def timetracer(myfun):

    @wraps(myfun)
    def _wrapper(*args, **kwargs):
        _t1 = tpt()
        _outcome = myfun(*args, **kwargs)
        _elapsed = tpt() - _t1
        print(f'{myfun.__name__} completed in {_elapsed} seconds')
        return _outcome
    return _wrapper
