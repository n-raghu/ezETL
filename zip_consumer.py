import sys
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed

from psycopg2 import connect as pgconnector

from dimlib import refresh_config


def zip_to_tbl():
    pass


def launchpad():
    pass


if __name__ == '__main__':
    print(refresh_config())
