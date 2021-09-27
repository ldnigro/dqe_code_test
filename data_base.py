"""
    Database and Spark module
    
    * init_spark():
    * stop_spark(spark:SparkSession):
    * set_current_db(spark:SparkSession, dbname: str):
    * drop_support_database(spark: SparkSession, dbname: str):
    * create_support_database(spark: SparkSession, dbpath: str, dbname: str):
    * insert_start_processed(spark: SparkSession, date: str, filename: str) -> bool:
    * update_end_processed(spark: SparkSession, date: str, filename: str) -> bool:

"""
from sqlite3.dbapi2 import Connection
from pyspark.sql import SparkSession
import sqlite3 as sl


def init_spark():
    """
        Init spark session
    """
    spark = SparkSession \
        .builder\
        .appName("zomato") \
        .master("local") \
        .enableHiveSupport() \
        .getOrCreate()

    return spark

def stop_spark(spark:SparkSession):
    """
        Stop spark session
    """
    try:
        spark.stop()
    except:
        pass

def connect(dbpath:str, dbname: str) -> Connection:
    """
        Connect to database
    """
    con = sl.connect("file:"+dbpath+dbname, uri=True)
    return con

def create_support_database(conn: Connection):
    """
        Create database for precessing registers\n
        * spark: spark session
        * dbpath: database location
        * dbname: database name
    """
    conn.execute("CREATE TABLE IF NOT EXISTS processed_files (filename TEXT, p_start TEXT, p_end TEXT)")
    conn.execute("CREATE TABLE IF NOT EXISTS process (p_day TEXT, p_start STRING, p_end STRING)")
    conn.commit()

def delete_support_database(conn: Connection):
    """
        Delete records from tables
    """
    try:
        conn.execute("DELETE FROM TABLE processed_files")
        conn.execute("DELETE FROM TABLE process")
        conn.commit()
    except:
        conn.rollback()

def insert_start_processed(conn: Connection, date: str, filename: str) -> bool:
    """
        Insert record for start process file

        Params
        * conn: connection
        * date 
        * filename: to process

        Return: True Ok, False Error
    """
    res = True
    try:
        conn.execute("INSERT INTO processed_files VALUES ('%s','%s','') "%(filename,date))
        conn.commit()
        res = True
    except:
        conn.rollback()
        res = False
    finally:
        return res


def update_end_processed(conn: Connection, date: str, filename: str) -> bool:
    """
        Update record for end process file

        Params
        * conn: Connection
        * date
        * filename: to process

        Return: True Ok, False Error
    """
    res = True
    try:
        conn.execute("UPDATE processed_files SET p_end='%s' where filename = '%s';"%(date,filename))
        conn.commit()
        res = True
    except:
        conn.rollback()
        res = False
    finally:
        return res

def can_process(conn: Connection, filename: str) -> bool:
    """
        Check if the file was processed
        if not, create a processing record

        Params:
        * conn: connection
        * filename\n

        Return: True if can process the file, False if not
    """
    curr = conn.execute("SELECT filename FROM processed_files where filename = '%s'"%filename )
    if curr.fetchone():
        return False
    return True

def insert_process_date(conn: Connection, date: str) -> bool:
    """
        Insert record for start process date

        Params
        * conn: connection
        * date 
        * filename: to process

        Return: True Ok, False Error
    """
    res = True
    try:
        conn.execute("INSERT INTO process VALUES ('%s','','') "%(date))
        conn.commit()
        res = True
    except:
        conn.rollback()
        res = False
    finally:
        return res

def update_process_date(conn: Connection, prev_date: str, date: str) -> bool:
    """
        Update record for start process date

        Params
        * conn: connection
        * prev_date
        * date 

        Return: True Ok, False Error
    """
    res = True
    try:
        conn.execute("UPDATE process SET p_day = '%s' WHERE p_date ='%s') "%(date,prev_date))
        conn.commit()
        res = True
    except:
        conn.rollback()
        res = False
    finally:
        return res

def get_last_date(conn: Connection) -> bool:
    """
        get last run

        Params
        * conn: connection

        Return: Last run or none
    """
    res = None
    try:
        cur = conn.execute("SELECT p_day FROM process")
        r = cur.fetchone()
        if r:
            res =  r[0]
    except:
        res = None
    finally:
        return res

        
