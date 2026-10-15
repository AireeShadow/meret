import logging
from logging.handlers import RotatingFileHandler


class Logger:
    def __init__(self, level: str = "DEBUG") -> None:
        self.logger = logging.getLogger("meret")
        self.logger.setLevel(getattr(logging, level.upper(), logging.DEBUG))

        if not self.logger.handlers:  # ← guard against duplicate handlers
            formatter = logging.Formatter(
                "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s"
            )
            handler = RotatingFileHandler(
                "meret.log", maxBytes=1_000_000, backupCount=10, encoding="utf-8"
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
