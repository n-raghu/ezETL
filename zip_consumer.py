import sys

from psycopg2 import connect as pgconnector

from dimlib import refresh_config

print(refresh_config())
