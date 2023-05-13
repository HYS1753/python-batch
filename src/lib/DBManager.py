import configparser
import os
import psycopg2
import pyodbc
import pymysql
import json
import logging
from psycopg2.extras import DictCursor

# env Settings
srcPath = os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
envConfig = configparser.ConfigParser()
envConfig.read(srcPath + '/config/db_properties.ini', encoding='UTF-8')

# logger setting
logger = logging.getLogger(__name__)

# PostgreSQL
class PgsqlConnection:

    # variable setting
    QUERYCOPYTO = 0
    TABLECOPYTO = 1
    COPYFROM = 2

    # 클래스 초기화
    def __init__(self, DB_HOST=None, DB_PORT=None, DB_NAME=None, DB_USER=None, DB_PASSWORD=None):
        # 아무 설정 값이 없으면 PGSQL_DB 기본 값으로 DB Connection 설정
        DB_HOST = envConfig['PGSQL']['DB_HOST'] if DB_HOST is None else DB_HOST
        DB_PORT = envConfig['PGSQL']['DB_PORT'] if DB_PORT is None else DB_PORT
        DB_NAME = envConfig['PGSQL']['DB_NAME'] if DB_NAME is None else DB_NAME
        DB_USER = envConfig['PGSQL']['DB_USER'] if DB_USER is None else DB_USER
        DB_PASSWORD = envConfig['PGSQL']['DB_PASSWORD'] if DB_PASSWORD is None else DB_PASSWORD

        conn_string = "host={0} port={1} dbname={2} user={3} password={4}" \
            .format(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD)

        logger.debug(f"DB Connection Info - HOST/PORT : {DB_HOST}:{DB_PORT} DATABASE : {DB_NAME} USER : {DB_USER}")
        self.conn = psycopg2.connect(conn_string)
        # 커서 딕셔너리 형태로 변경
        self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def __del__(self):
        if self.cursor is not None:
            self.cursor.close()
        self.conn.close()
        logger.debug("DB Connection close")

    def excute_read_qry(self, query, args=None, col_info=False):
        if args is None:
            args = ()
        self.cursor.execute(query, args)
        row = self.cursor.fetchall()
        if col_info:
            column_names = [desc[0] for desc in self.cursor.description]
            return row, column_names
        else:
            return row

    # Auto commit
    def excute_write_qry(self, query, args=None):
        if args is None:
            args = ()
        with self.conn:
            with self.cursor as cur:
                return cur.execute(query, args)

    def not_auto_commit_excute_write_qry(self, query, args=None):
        if args is None:
            args = ()
        return self.cursor.execute(query, args)

    def call_proc(self, procedure_name, params= None):
        with self.conn:
            with self.cursor as cur:
                cur.callproc(procedure_name, params)
                out_args = cur.fetchall()
                return out_args

    # copy_expert(this method can use both of copy_to, copy_from method)
    # from psycopg2==2.9.0 can't use copy_to, copy_from method with schema.
    # PgSQL Copy(default col_delimiter = \t, row_delimiter = \n)
    # option 0 = copy_to, option 1 = copy_from
    def copy_expert(self, option, target, file_path):
        result = False
        with self.conn:
            with self.cursor as cur:
                if option == self.QUERYCOPYTO:
                    infile = open(file_path, 'w')
                    cur.copy_expert(sql=f"COPY ({target}) TO STDOUT", file=infile)
                elif option == self.TABLECOPYTO:
                    infile = open(file_path, 'w')
                    cur.copy_expert(sql=f"COPY {target} TO STDOUT", file=infile)
                elif option == self.COPYFROM:
                    infile = open(file_path, 'r')
                    cur.copy_expert(sql=f"COPY {target} FROM STDIN NULL AS 'None'", file=infile)
                else:
                    logger.debug("Incorrect copy_expert option")
                    return result
        return True

# Mysql
class MysqlConnection:

    # 클래스 초기화
    def __init__(self, DB_HOST=None, DB_PORT=None, DB_NAME=None, DB_USER=None, DB_PASSWORD=None, DB_CHARSET=None):
        # 아무 설정 값이 없으면 PGSQL_DB 기본 값으로 DB Connection 설정
        DB_HOST = envConfig['MYSQL']['DB_HOST'] if DB_HOST is None else DB_HOST
        DB_PORT = envConfig['MYSQL']['DB_PORT'] if DB_PORT is None else DB_PORT
        DB_NAME = envConfig['MYSQL']['DB_NAME'] if DB_NAME is None else DB_NAME
        DB_USER = envConfig['MYSQL']['DB_USER'] if DB_USER is None else DB_USER
        DB_PASSWORD = envConfig['MYSQL']['DB_PASSWORD'] if DB_PASSWORD is None else DB_PASSWORD
        DB_CHARSET = envConfig['MYSQL']['DB_CHARSET'] if DB_CHARSET is None else DB_CHARSET # ex.utf8

        # pymysql의 connect() 메소드는 string type connection info 파싱 지원 안해 직접 삽입.
        # conn_string = "host='{0}', port={1}, db='{2}', user='{3}', passwd='{4}', charset='{5}'" \
        #     .format(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DB_CHARSET)

        logger.debug(f"DB Connection Info - HOST/PORT : {DB_HOST}:{DB_PORT} DATABASE : {DB_NAME} USER : {DB_USER}")
        self.conn = pymysql.connect(
            host=DB_HOST,
            port=int(DB_PORT),
            db=DB_NAME,
            user=DB_USER,
            passwd=DB_PASSWORD,
            charset=DB_CHARSET)
        # 커서 생성
        self.cursor = self.conn.cursor()

    def __del__(self):
        if self.cursor is not None:
            self.cursor.close()
        self.conn.close()
        logger.debug("DB Connection close")

    def excute_read_qry(self, query, args=None, col_info=None):
        if args is None:
            args = ()
        if col_info is None:
            col_info = False
        self.cursor.execute(query, args)
        row = self.cursor.fetchall()
        if col_info:
            column_names = [desc[0] for desc in self.cursor.description]
            return row, column_names
        else:
            return row

    # Auto commit
    def excute_write_qry(self, query, args=None):
        if args is None:
            args = ()
        with self.conn:
            with self.cursor as cur:
                return cur.execute(query, args)

# Sybase
class SybaseConnection:

    # 클래스 초기화
    def __init__(self, DB_HOST=None, DB_PORT=None, DB_NAME=None, DB_USER=None, DB_PASSWORD=None, DB_DRIVER=None, DB_CHARSET=None):
        # 아무 설정 값이 없으면 PGSQL_DB 기본 값으로 DB Connection 설정
        DB_HOST = envConfig['SYBASE']['DB_HOST'] if DB_HOST is None else DB_HOST
        DB_PORT = envConfig['SYBASE']['DB_PORT'] if DB_PORT is None else DB_PORT
        DB_NAME = envConfig['SYBASE']['DB_NAME'] if DB_NAME is None else DB_NAME
        DB_USER = envConfig['SYBASE']['DB_USER'] if DB_USER is None else DB_USER
        DB_PASSWORD = envConfig['SYBASE']['DB_PASSWORD'] if DB_PASSWORD is None else DB_PASSWORD
        DB_DRIVER = envConfig['SYBASE']['DB_DRIVER'] if DB_DRIVER is None else DB_DRIVER # ex.{Adaptive Server Enterprise}
        DB_CHARSET = envConfig['SYBASE']['DB_CHARSET'] if DB_CHARSET is None else DB_CHARSET # ex.eucksc

        conn_string = "driver={0};server={1};database={2};port={3};uid={4};pwd={5};charset={6}" \
            .format(DB_DRIVER, DB_HOST, DB_NAME, DB_PORT, DB_USER, DB_PASSWORD, DB_CHARSET)

        logger.debug(f"DB Connection Info - HOST/PORT : {DB_HOST}:{DB_PORT} DATABASE : {DB_NAME} USER : {DB_USER}")
        self.conn = pyodbc.connect(conn_string)
        # 커서 생성
        self.cursor = self.conn.cursor()

    def __del__(self):
        if self.cursor is not None:
            self.cursor.close()
        self.conn.close()
        logger.debug("DB Connection close")

    def excute_read_qry(self, query, args=None, col_info=None):
        if args is None:
            args = ()
        if col_info is None:
            col_info = False
        self.cursor.execute(query, args)
        row = self.cursor.fetchall()
        if col_info:
            column_names = [desc[0] for desc in self.cursor.description]
            return row, column_names
        else:
            return row

    # Auto commit
    def excute_write_qry(self, query, args=None):
        if args is None:
            args = ()
        with self.conn:
            with self.cursor as cur:
                return cur.execute(query, args)


def main():
    # POSTGRESQL
    # db = PgsqlConnection()
    # query = """select * from dev.tb_road_name_addr trna limit 2"""
    # try:
    #     if db.conn.closed != 0:
    #         db = PgsqlConnection()
    #     rows = db.excute_read_qry(query)
    #     print(rows)
    #     print("-----------------------------------------------")
    #     rows, columns = db.excute_read_qry(query, col_info=True)
    #     print(rows)
    #     print(columns)
    # except Exception as e:
    #     print(f"Error occurred : {str(e)}")
    # finally:
    #     db.conn.commit()
    #     del(db)

    # MYSQL
    db = MysqlConnection()
    query = """select * from test limit 2"""
    try:
        if not db.conn.open:
            db = MysqlConnection()
        rows = db.excute_read_qry(query)
        print(rows)
        print("-----------------------------------------------")
        rows, columns = db.excute_read_qry(query, col_info=True)
        print(rows)
        print(columns)
    except Exception as e:
        print(f"Error occurred : {str(e)}")
    finally:
        db.conn.commit()
        del(db)

    # SYBASE
    # db = SybaseConnection()
    # query = """select top 10 * from dbo.TM_CMDT AT ISOLATION 0"""
    # try:
    #     if db.conn.closed != 0:
    #         db = SybaseConnection()
    #     rows = db.excute_read_qry(query)
    #     print(rows)
    #     print("-----------------------------------------------")
    #     rows, columns = db.excute_read_qry(query, col_info=True)
    #     print(rows)
    #     print(columns)
    # except Exception as e:
    #     print(f"Error occurred : {str(e)}")
    # finally:
    #     db.conn.commit()
    #     del(db)

    pass

if __name__ == "__main__":
    main()