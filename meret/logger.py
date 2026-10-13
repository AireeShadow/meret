import logging
from logging.handlers import RotatingFileHandler

class Logger():
    '''
    Class with logger
    Meant to be a parent class
    '''
    def __init__(self, level: str='DEBUG') -> None:
        self.logger = logging.getLogger('meret')
        match level:
            case 'DEBUG':
                self.logger.setLevel(logging.DEBUG)
            case 'INFO':
                self.logger.setLevel(logging.INFO)
            case 'WARNING' | 'WARN':
                self.logger.setLevel(logging.WARNING)
            case 'ERROR':
                self.logger.setLevel(logging.ERROR)
            case "CRITICAL":
                self.logger.setLevel(logging.CRITICAL)
            case _:
                raise RuntimeError(f'Wrong logging level: {level}, aborting')
        formatter = logging.Formatter('[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s')
        logHandler = RotatingFileHandler('meret.log', maxBytes=100000, backupCount=10)
        logHandler.setLevel(logging.DEBUG)
        logHandler.setFormatter(formatter)
        self.logger.addHandler(logHandler)