import os
import logging
import time
from DBManager import SybaseConnection, PgsqlConnection

# copy file 형태로 저장 프로세스
# postgresql default sep : .txt('\t', '\n'), .csv(',', '\n')
# Common defualt sep  : .sql('||||', '\n')

# logger Setting
logger = logging.getLogger(__name__)
src_path = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))

class DB2ESBulkConvertor:
    def __init__(self, db, fields=None, lines=None):
        logger.debug("DB2ESBulkConvertor start.")

        # Copy File 의 구분자 설정 (default : fields(\t), lines(\n))
        self.fields = "\t" if fields is None else fields
        self.lines = "\n" if lines is None else lines
        # DB 설정
        self.db = db

        print(f"Copy file delimiter set each {self.fields} and {self.lines}")

    def __del__(self):
        logger.debug("DB2ESBulkConvertor end.")

    def generate_bulkfile(self, query, filename):
        file_path = os.path.join(src_path, filename)
        logger.debug(f"Generate Bulk file start. File path : {file_path}")
        result = False
        columns = []
        try:
            rows, columns = self.db.excute_read_qry(query, col_info=True)
            if len(rows) > 0:
                result_count = 0
                s_time = time.time()
                with open(file_path, 'w') as wf:
                    for row in rows:
                        for index, data in enumerate(row):
                            if type(data) == type(''):
                                data = data.replace('\\', '')
                                data = data.replace('\n', '\\n')
                            if index == 0:
                                serial_data = f"{data}"
                            else:
                                serial_data += f"{self.fields}{data}"
                        wf.write(serial_data + f"{self.lines}")
                        result_count += 1
                c_time = time.time() - s_time
                result = True
                logger.debug(f"Generate Bulk file end. Generated {result_count}, elapse time: {c_time}")
            else:
                logger.debug(f"Nothing to generate Bulk file.")
        except Exception as e:
            logger.debug(f"{str(e)}")
        return result, columns


def main():
    query = """select * from dev.tb_road_name_addr trna limit 5"""
    db = PgsqlConnection()
    try:
        if db.conn.closed != 0:
            db = PgsqlConnection()
        gen = DB2ESBulkConvertor(db=db, fields="$$^^||", lines="\n")
        result, columns = gen.generate_bulkfile(query, os.path.join('data', 'es_data', 'test_es.txt'))
        print(result)
        print(columns)
    except Exception as e:
        print(f"Error occurred : {str(e)}")
    finally:
        db.conn.commit()
        del (db)


if __name__ == "__main__":
    main()