import sys

from psycopg2 import connect as pgconnector

from dimlib import refresh_config

cfg = refresh_config()
cnx = pgconnector(cfg['dburi'])

try:
    action = sys.argv[1].lower()
except Exception as err:
    action = 'check'

d_users = '''
        CREATE TABLE IF NOT EXISTS users(
            userid TEXT,
            created_stamp TIMESTAMP WITHOUT TIME ZONE,
            instancecode TEXT
        );
    '''
if action == 'purge':
    d_users = 'DROP TABLE IF EXISTS users CASCADE;'
elif action == 'recreate':
    d_users = f'DROP TABLE IF EXISTS users CASCADE;{d_users}'


d_courses = '''
        CREATE TABLE IF NOT EXISTS courses(
            courseid TEXT,
            created_stamp TIMESTAMP WITHOUT TIME ZONE,
            INSTANCECODE TEXT
        );
    '''
if action == 'purge':
    d_courses = 'DROP TABLE IF EXISTS courses CASCADE;'
elif action == 'recreate':
    d_courses = f'DROP TABLE IF EXISTS courses;{d_courses}'


f_usercourses = '''
        CREATE TABLE IF NOT EXISTS userassignedcourses(
            courseid TEXT,
            userid TEXT,
            created_stamp TIMESTAMP WITHOUT TIME ZONE,
            INSTANCECODE TEXT
        );
    '''
if action == 'purge':
    f_usercourses = 'DROP TABLE IF EXISTS userassignedcourses CASCADE;'
elif action == 'recreate':
    f_usercourses = f'DROP TABLE IF EXISTS userassignedcourses CASCADE;{f_usercourses}'

with cnx.cursor() as dbcur:
    dbcur.execute(d_users)
    dbcur.execute(d_courses)
    dbcur.execute(f_usercourses)
cnx.commit()
cnx.close()
