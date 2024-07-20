import logging
from logging.handlers import RotatingFileHandler

class Logger():
    '''
    Class with logger
    Meant to be a parent class
    '''
    def __init__(self) -> None:
        # self.logger = logging.basicConfig(
        #     handlers=[RotatingFileHandler('../meret.log', maxBytes=100000, backupCount=10)],
        #     level=logging.DEBUG,
        #     format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
        #     datefmt='%Y-%m-%dT%H:%M:%S'
        # )
        self.logger = logging.getLogger('meret')
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter('[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s')
        logHandler = RotatingFileHandler('meret.log', maxBytes=100000, backupCount=10)
        logHandler.setLevel(logging.DEBUG)
        logHandler.setFormatter(formatter)
        self.logger.addHandler(logHandler)