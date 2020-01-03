from dimlib import *

key = 'worker/nuset/24DCCFCAA2DC0B2FB7BB35DBB1A768A71FAC7066.asc'
file_name = 'worker/20191218_lms.zip.gpg'

with open(key) as kfile:
    k = kfile.read()

gpg = GPG()
gpg.import_keys(k)

print(file_decrypter(file_name, 'SFASFASFA', gpg))
