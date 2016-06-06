import tornado
from tornado.util import basestring_type, exec_in
from tornado.escape import _unicode, native_str
from tornado.options import options, define
import random
import string

import pymysql

def parse_config_file(path, final=True):

    """Parses and loads the Python config file at the given path.

    This version allow customize new options which are not defined before
    from a configuration file.
    """
    config = {}
    with open(path, 'rb') as f:
        exec_in(native_str(f.read()), {}, config)
    for name in config:
        normalized = options._normalize_name(name)
        if normalized in options._options:
            options._options[normalized].set(config[name])
        else:
            tornado.options.define(name, config[name])
    if final:
        options.run_parse_callbacks()

def generateURL(size = 16, chars = string.ascii_lowercase + string.digits):
    return ''.join(random.SystemRandom().choice(chars) for _ in range(size))


def makePwnedConnection(conn):
    commit = conn.commit
    cursor = conn.cursor
    
    def new_commit():
        try:
            return commit()
        except pymysql.err.OperationalError as e:
            print("MySQL error, recreating connection")
            server = options.mysql["server"]
            user = options.mysql["user"]
            password = options.mysql["password"]
            database = options.mysql["database"]
            connection = pymysql.connect(host=server, user=user, password=password, db=database,cursorclass=pymysql.cursors.DictCursor, charset='utf8')
            conn = makePwnedConnection(connection)
            normalized = options._normalize_name("connection")
            if normalized in options._options:
                options._options[normalized].set(connection)
            print("Connection recreated")
            return conn.commit()
            
    def new_cursor():
        try:
            return cursor()
        except pymysql.err.OperationalError as e:
            print("MySQL error, recreating connection")
            server = options.mysql["server"]
            user = options.mysql["user"]
            password = options.mysql["password"]
            database = options.mysql["database"]
            connection = pymysql.connect(host=server, user=user, password=password, db=database,cursorclass=pymysql.cursors.DictCursor, charset='utf8')
            conn = makePwnedConnection(connection)
            normalized = options._normalize_name("connection")
            if normalized in options._options:
                options._options[normalized].set(connection)
            print("Connection recreated")
            
            
    conn.__dict__["commit"] = new_commit
    conn.__dict__["cursor"] = new_cursor
    return conn
