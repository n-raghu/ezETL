import sys

from dimlib import timetracer


def sql_users(action):
    d_users = '''
            CREATE TABLE IF NOT EXISTS users(
                userid TEXT,
                instancecode TEXT,
                rower BIGINT,
                row_timestamp TIMESTAMP WITHOUT TIME ZONE
            );
        '''
    if action == 'purge':
        d_users = 'DROP TABLE IF EXISTS users CASCADE;'
    elif action == 'recreate':
        d_users = f'DROP TABLE IF EXISTS users CASCADE;{d_users}'
    return d_users


def sql_courses(action):
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
        d_courses = f'DROP TABLE IF EXISTS courses CASCADE;{d_courses}'
    return d_courses


def sql_usercourses(action):
    f_usercourses = '''
            CREATE TABLE IF NOT EXISTS user_assigned_courses(
                instancecode TEXT
            );
        '''
    if action == 'purge':
        f_usercourses = 'DROP TABLE IF EXISTS user_assigned_courses CASCADE;'
    elif action == 'recreate':
        f_usercourses = f'DROP TABLE IF EXISTS user_assigned_courses CASCADE;{f_usercourses}'
    return f_usercourses


@timetracer
def mother_tables(cnx, action):
    d_users = sql_users(action)
    d_courses = sql_courses(action)
    f_usercourses = sql_usercourses(action)
    with cnx.cursor() as dbcur:
        dbcur.execute(d_users)
        dbcur.execute(d_courses)
        dbcur.execute(f_usercourses)
    cnx.commit()
