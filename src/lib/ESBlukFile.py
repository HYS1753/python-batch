import os
import time
import logging
import configparser
from elasticsearch import helpers
from ESManager import ESConnection

# env Settings
srcPath = os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
envConfig = configparser.ConfigParser()
envConfig.read(srcPath + '/config/es_properties.ini', encoding='UTF-8')

# logger setting
logger = logging.getLogger(__name__)

# User Exception
class InitError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg

# ElasticSearch
class ESBulkFile:

    # 클래스 초기화
    def __init__(self, ES=None, INDEX=None, SCHEME=None, FILE_PATH=None, SEP=None):
        try:
            # variable check
            if ES is None or INDEX is None or SCHEME is None or FILE_PATH is None:
                raise InitError("ES: {0}, INDEX: {1}, SCHEME: {2}, FILE_PATH: {3}"
                                .format(ES is not None, INDEX is not None, SCHEME is not None, FILE_PATH is not None))
            self.es = ES
            self.index = INDEX
            self.scheme= SCHEME
            self.file_path = FILE_PATH
            self.sep = '\t' if SEP is None else SEP

        except InitError as err:
            print("Variable initialize error. [" + str(err) + "]")

    def __del__(self):
        logger.debug("ESBulkFile class destroyed")

    def generateSourceError(self):
        print("error")

    def generate_docs(self):
        with open(os.path.join(srcPath, self.file_path), "r") as file:
            while True:
                line = file.readline().strip()
                # file 읽기 종료
                if not line: break
                # source 생성
                fields = line.split(self.sep)
                source = dict()
                if len(self.scheme) == len(fields):
                    for index, key in enumerate (self.scheme):
                        source[key] = fields[index]
                else:
                    self.generateSourceError()
                    continue
                # document 생성
                doc = dict()
                doc["_index"] = self.index
                doc["_id"] = fields[0]
                doc["_source"] = source
                #print(doc)
                yield doc

    def file_bulk(self):
        print("Elasticsearch Bulk API with file start.")
        s_time = time.time()
        result, msg = helpers.bulk(self.es, self.generate_docs())
        print(f"Elasticsearch Bulk API with file end. elapse time : {time.time() - s_time}")
        return result

def main():
    # https://elasticsearch-py.readthedocs.io/en/master/api.html
    es = ESConnection()
    scheme = ['rna_mgmt_num', 'legal_dong_code', 'sido', 'sigungu', 'eupmyeondong', 'li',
             'san_ysno', 'land_num', 'ho', 'rna_code', 'road_name', 'basement_ysno',
             'bldg_main_num', 'bldg_sub_num', 'admin_dong_code', 'admin_dong_name',
             'zip_code', 'prev_rna', 'effect_date', 'apt_ysno', 'bldg_name',
             'cret_id', 'cret_dttm', 'amnd_id', 'amnd_dttm']
    bulk = ESBulkFile(ES=es.conn, INDEX="bulk_test", SCHEME=scheme,
                      FILE_PATH=os.path.join('data', 'es_data', 'test_es.txt'),
                      SEP="$$^^||")
    # bulk.generate_docs()
    result = bulk.file_bulk()
    print(str(result))
    result = es.conn.count(index="bulk_test")
    print(result.body['count'])

if __name__ == "__main__":
    main()