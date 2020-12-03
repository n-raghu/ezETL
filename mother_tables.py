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
            cc TEXT,
            created_stamp TIMESTAMP WITHOUT TIME ZONE,
            instancecode TEXT
        );
    '''
if action == 'purge':
    d_users = 'DROP TABLE IF EXISTS users;'
elif action == 'recreate':
    d_users = f'DROP TABLE IF EXISTS users;{d_users}'


d_courses = '''
        CREATE TABLE IF NOT EXISTS courses(
            courseid TEXT,
            strCopiedFromUserDefinedCourseID TEXT,
            created_stamp TIMESTAMP WITHOUT TIME ZONE,
            INSTANCECODE TEXT
        );
    '''
if action == 'purge':
    d_users = 'DROP TABLE IF EXISTS courses;'
elif action == 'recreate':
    d_users = f'DROP TABLE IF EXISTS courses;{d_courses}'


f_usercourses = '''
        CREATE TABLE IF NOT EXISTS userassignedcourses(
            courseid TEXT,
            userid TEXT,
            created_stamp TIMESTAMP WITHOUT TIME ZONE,
            INSTANCECODE TEXT
        );
    '''
if action == 'purge':
    d_users = 'DROP TABLE IF EXISTS usercourses;'
elif action == 'recreate':
    d_users = f'DROP TABLE IF EXISTS usercourses;{f_usercourses}'

with cnx.cursor() as dbcur:
    dbcur.execute(d_users)
    dbcur.execute(d_courses)
    dbcur.execute(f_usercourses)
cnx.commit()
cnx.close()
