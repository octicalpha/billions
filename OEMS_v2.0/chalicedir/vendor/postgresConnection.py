import os
import sys
import psycopg2

if sys.version_info >= (3, 0):
    from urllib.parse import urlparse as urlparser
elif sys.version_info >= (2, 0):  # but less than 3
    import urlparse

    urlparse.uses_netloc.append("postgres")
    urlparser = urlparse.urlparse
from sqlalchemy import create_engine
import time

DATABASE_URL = "postgres://awsuser:AGvafsdgaSDG1232@db1.civby46zvtth.us-east-2.rds.amazonaws.com:5432/mydb"


def _get_conn(environ_str):
    try:
        url = urlparser(environ_str)
        return psycopg2.connect(
            database=url.path[1:],
            user=url.username,
            password=url.password,
            host=url.hostname,
            port=url.port
        )
    except:
        return None


conn = _get_conn(DATABASE_URL)
engine = create_engine(DATABASE_URL)


def getEngine(environment):
    return {
        'prod': engine,
    }[environment.lower()]


def getConn(environment):
    return {
        'prod': conn
    }[environment.lower()]


def commit(environment):
    conn = getConn(environment)
    conn.commit()


def getCursor(environment):
    conn = getConn(environment)
    cur = conn.cursor()
    return cur


def insert(sql, environment):
    cur = getCursor(environment)
    cur.execute(sql)
    commit(environment)


def query(sql, params=None, environment="prod"):
    cur = getCursor(environment)
    if params is not None:
        cur.execute(sql, params)
    else:
        cur.execute(sql)
    return cur.fetchall()


def runCommand(sql, environment):
    cur = getCursor(environment)
    cur.execute(sql)
    return commit(environment)


def getCurrentDateAndTime():
    return time.strftime("%Y%m%dT%H%M%S")


def storeInDb(data, tableName, environment, engine=None, index=False, index_label=None, addMilestoneCol=True,
              schema=None):
    if engine == None:
        engine = getEngine(environment)
    insert(
        """INSERT into {}.{} (name, maxnonce, ip, in_z) values ('{}',{},'{}',{});""".format(schema, tableName, data[0],
                                                                                            data[1], data[2],
                                                                                            time.time()), environment)


def table_exits(schema_name, table_name, environment):
    sql = "select exists( select 1 from information_schema.tables where table_schema = %s and table_name = %s );"
    return query(sql, params=(schema_name, table_name), environment=environment)[0][0]
