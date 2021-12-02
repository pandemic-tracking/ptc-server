"""This is where we defined the Config files, which are used for initiating the
application with specific settings such as logger configurations or different
database setups."""

from app.utils.logging import file_logger, client_logger
from decouple import config as env_conf
import logging


class LocalConfig:
    SECRET_KEY = env_conf("SECRET_KEY", cast=str, default="12345")

    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_RECORD_QUERIES = True

    SNAPSHOT_SHEET_ID = env_conf('SNAPSHOT_SHEET_ID', cast=str, default='')
    CREDENTIALS_PATH = env_conf('CREDENTIALS_PATH', cast=str, default='')
    CHECKS_SHEET_ID = env_conf('CHECKS_SHEET_ID', cast=str, default='')

    @staticmethod
    def init_app(app):
        # The default Flask logger level is set at ERROR, so if you want to see
        # INFO level or DEBUG level logs, you need to lower the main loggers
        # level first.
        app.logger.setLevel(logging.DEBUG)
        app.logger.handlers.clear()
        app.logger.addHandler(file_logger)
        app.logger.addHandler(client_logger)
