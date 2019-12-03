import os
import sys
from glob import iglob
from zipfile import ZipFile
from time import perf_counter as tpc
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed as fork_complete

from yaml import safe_load as yml_safe_load
from psycopg2 import connect as pgconnector


def error_trace():
    pass


def dimlogger():
    pass
