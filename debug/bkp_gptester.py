import gnupg
from zipfile import ZipFile

zfile = 'worker/20191212.zip'
with open('worker/key.asc') as kfile:
    key = kfile.read()

gpg = gnupg.GPG()
gpg.import_keys(key)

with open('worker/2019_lms.zip.gpg', 'rb') as gfile:
    gpg.decrypt_file(gfile, passphrase='TESTTESTTEST', output=zfile)

with ZipFile(zfile, 'r') as zet:
    zet.setpassword(b'eaemaster')
