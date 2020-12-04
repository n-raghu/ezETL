import sys
from time import time, sleep

try:
    sleep_seconds = int(sys.argv[1])
except Exception:
    sleep_seconds = 696969

sleep(sleep_seconds)
