import psycopg2
from configparser import ConfigParser
import os


def config(filename='database.ini', section='postgresql'):
    parser = ConfigParser()
    scriptpath = os.path.dirname(__file__)
    filename = os.path.join(scriptpath, filename)
    parser.read(filename)
    db = {}

    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


def connector(f):
    def wrapper_func(*args, **kwargs):
        """ Connect to the PostgreSQL database server """
        conn = None
        try:
            # read connection parameters
            params = config()
            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            conn = psycopg2.connect(**params)
            kwargs['conn'] = conn
            return f(*args, **kwargs)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')
    return wrapper_func

