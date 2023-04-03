import os
import logging

# copy file 형태로 저장 프로세스
# postgresql default sep : .txt('\t', '\n'), .csv(',', '\n')
# Common defualt sep  : .sql('||||', '\n')

# logger Setting
logger = logging.getLogger(__name__)

class DB2FileConverter:
    def __init__(self, fields=None, lines=None):
        logger.debug("DB2FileConverter start.")

        # Copy File 의 구분자 설정 (default : fields(\t), lines(\n))
        self.fields = "\t" if fields is not None else fields
        self.lines = "\n" if lines is not None else lines

        logger.debug(f"Copy file delimiter set each {self.fields} and {self.lines}")

    def __del__(self):
        logger.debug("DB2FileConverter end.")

