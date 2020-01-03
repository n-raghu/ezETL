from io import BytesIO
from zipfile import ZipFile

import gnupg

zfile = 'worker/20191212.zip'
with open('worker/key.asc') as kfile:
    key = kfile.read()

gpg = gnupg.GPG()
gpg.import_keys(key)
zip_file_obj = BytesIO()

with open('worker/2019_lms.zip.gpg', 'rb') as gfile:
    decrypt_data = gpg.decrypt_file(gfile, passphrase='TESTTESTTEST')

zip_file_obj = BytesIO(decrypt_data.data)
