import logging
from dataclasses import dataclass


@dataclass
class Logger:
    name: str
    level: int

    def return_logger(self):
        logger = logging.getLogger(self.name)
        logger.setLevel(self.level)

        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', "%Y-%m-%d %H:%M:%S")

        ch.setFormatter(formatter)
        logger.addHandler(ch)

        return logger
