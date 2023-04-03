import logging
import datetime
import os

srcPath = os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
logPath = os.path.join(srcPath, "logs")
nowDate = datetime.datetime.now().strftime("%Y%m%d")
class LogManager:
    # method [0: console only, 1: file only, 2: console + file]
    def __init__(self, METHOD=None, FILENAME=None, LEVEL=None):

        METHOD = 2 if METHOD is None else METHOD
        PATH = os.path.join(logPath, f"{nowDate}-python-batch.log") if FILENAME is None else os.path.join(logPath, f"{nowDate}-{FILENAME}.log")
        LEVEL = "DEBUG" if LEVEL is None else LEVEL

        # Make root logger
        self.logger = logging.getLogger()

        # Setting log level
        if (LEVEL == "DEBUG") or (LEVEL == "debug"):
            self.logger.setLevel(logging.DEBUG)
        elif (LEVEL == "INFO") or (LEVEL == "info"):
            self.logger.setLevel(logging.INFO)
        elif (LEVEL == "ERROR") or (LEVEL == "error"):
            self.logger.setLevel(logging.ERROR)
        elif (LEVEL == "CRITICAL") or (LEVEL == "critical"):
            self.logger.setLevel(logging.CRITICAL)
        else:
            self.logger.setLevel(logging.WARN)

        # Setting log location
        # Console Only
        if METHOD == 0:
            self.set_console_log()
        # File Only
        elif METHOD == 1:
            self.set_file_log(PATH)
        # Console + File
        else:
            self.set_console_log()
            self.set_file_log(PATH)

        if self.logger is not None:
            self.trace_log("debug", f"LogManager start. LEVEL = {LEVEL}, PATH = {PATH}, METHOD = {METHOD}")

    def __del__(self):
        if self.logger is not None:
            self.trace_log("debug", "LogManager end")

    def set_console_log(self):
        console = logging.StreamHandler()
        # set formatter
        formatter = logging.Formatter(fmt='%(asctime)s %(levelname)s [%(name)s] [%(filename)s:%(lineno)d] %(message)s',
                                      datefmt='%Y-%m-%d %H:%M:%S')
        console.setFormatter(formatter)
        self.logger.addHandler(console)

    def set_file_log(self, path):
        file_handler = logging.FileHandler(filename=path)
        # set formatter # default '[%(asctime)s] [%(levelname)s] [%(filename)s] %(message)s'
        formatter = logging.Formatter(fmt='%(asctime)s %(levelname)s [%(name)s] [%(filename)s:%(lineno)d] %(message)s',
                                      datefmt='%Y-%m-%d %H:%M:%S')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def trace_log(self, log_level, message):
        if (log_level == "CRITICAL") or (log_level == "critical"):
            self.logger.critical(message)
        elif (log_level == "ERROR") or (log_level == "error"):
            self.logger.error(message)
        elif (log_level == "WARNING") or (log_level == "warning") or (log_level == "WARN") or (log_level == "warn"):
            self.logger.warn(message)
        elif (log_level == "INFO") or (log_level == "info"):
            self.logger.info(message)
        else:
            self.logger.debug(message)


def main():
    logger = LogManager(FILENAME='test')
    logger.trace_log("debug", "hello")

if __name__ == '__main__':
    main()
