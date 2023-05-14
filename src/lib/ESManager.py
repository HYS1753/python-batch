import os
import logging
import configparser
import base64
from elasticsearch import Elasticsearch

# env Settings
srcPath = os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
envConfig = configparser.ConfigParser()
envConfig.read(srcPath + '/config/es_properties.ini', encoding='UTF-8')

# logger setting
logger = logging.getLogger(__name__)

# ElasticSearch
class ESConnection:

    # 클래스 초기화
    def __init__(self, ES_HOST=None, ES_PORT=None, ES_API_ID=None, ES_API_KEY=None, ES_USER=None, ES_PASSWORD=None, ES_CERT_PATH=None):
        # 아무 설정 값이 없으면 PGSQL_DB 기본 값으로 DB Connection 설정
        ES_HOST = envConfig['ELASTICSEARCH']['ES_HOST'] if ES_HOST is None else ES_HOST
        ES_PORT = envConfig['ELASTICSEARCH']['ES_PORT'] if ES_PORT is None else ES_PORT
        ES_API_ID = envConfig['ELASTICSEARCH']['ES_API_ID'] if ES_API_ID is None else ES_API_ID
        ES_API_KEY = envConfig['ELASTICSEARCH']['ES_API_KEY'] if ES_API_KEY is None else ES_API_KEY
        ES_USER = envConfig['ELASTICSEARCH']['ES_USER'] if ES_USER is None else ES_USER
        ES_PASSWORD = envConfig['ELASTICSEARCH']['ES_PASSWORD'] if ES_PASSWORD is None else ES_PASSWORD
        ES_CERT_PATH = envConfig['ELASTICSEARCH']['ES_CERT_PATH'] if ES_CERT_PATH is None else ES_CERT_PATH

        # API_KEY 사용 안할 시 ES USER로 접속
        # API_KEY 생성 URI(https://{{ElasticSearch}}/_security/api_key)
        if (ES_API_ID is None) or (ES_API_ID == ''):
            logger.debug(f"ES Connection Info - HOST/PORT : {ES_HOST}:{ES_PORT}, AUTH with BASIC AUTH : {ES_USER}")
            self.conn = Elasticsearch(ES_HOST+':'+ES_PORT,
                               ca_certs=srcPath+ES_CERT_PATH,
                               verify_certs=False,
                               ssl_show_warn=False,
                               basic_auth=(ES_USER, ES_PASSWORD))
        else:
            logger.debug(f"ES Connection Info - HOST/PORT : {ES_HOST}:{ES_PORT}, AUTH with API_KEY : {ES_API_ID}")
            self.conn = Elasticsearch(ES_HOST + ':' + ES_PORT,
                               ca_certs=srcPath + ES_CERT_PATH,
                               verify_certs=False,
                               ssl_show_warn=False,
                               api_key=(ES_API_ID, ES_API_KEY))

    def __del__(self):
        if self.conn is not None:
            self.conn.close()
        logger.debug("ES Connection close")

def main():
    # https://elasticsearch-py.readthedocs.io/en/master/api.html
    es = ESConnection()

    health = es.conn.cat.health()
    print(health)
    print(es.conn)
    # print(es)

if __name__ == "__main__":
    main()
