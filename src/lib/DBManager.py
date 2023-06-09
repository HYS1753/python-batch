import configparser
import os
import psycopg2
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

    def excute_read_qry(self, query, args=None):
        if args is None:
            args = ()
        self.cursor.execute(query, args)
        row = self.cursor.fetchall()
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

def main():
    db = PgsqlConnection()
    query = """select * from dev.tb_road_name_addr trna limit 10"""
    try:
        if db.conn.closed != 0:
            db = PgsqlConnection()
        rows = db.excute_read_qry(query)
        print(rows)
    except Exception as e:
        print(f"Error occurred : {str(e)}")

if __name__ == "__main__":
    main()