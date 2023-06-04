import logging
import os
from global_utils import make_dir

CURRENT_FOLDER_NAME = os.path.dirname(os.path.abspath(__file__))


class Logger:
    def __init__(self, log_file_name: str, module_name: str):
        """
        :param log_file_name: name of the log file
        :param module_name: name of the module (can be kept same as the log_file_name without the extension)
        """
        # Create a custom logger
        self.logger = logging.getLogger(module_name)
        make_dir(directory=os.path.join(CURRENT_FOLDER_NAME, 'logs'))

        # basic class from logging model
        self.f_handler = logging.FileHandler(os.path.join(CURRENT_FOLDER_NAME, 'logs', log_file_name))

        # Create formatters and add it to handlers
        # format of how output log should look
        # time, name, servertity level, message itsel
        format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        # set the logging formatter to the f_handler
        # attach my newnly created format to the file handler 
        self.f_handler.setFormatter(format)
        # Add handlers to the logger and setlevel to DEBUG
        self.logger.addHandler(self.f_handler)
        # set severtity level
        self.logger.setLevel(logging.DEBUG)

    def warning(self, msg):
        self.logger.warning(msg)


    def error(self, msg):
        self.logger.error(msg)

    def info(self, msg):
        self.logger.info(msg)

    def debug(self, msg):
        self.logger.debug(msg)



server_logger = Logger(log_file_name='server_logs.txt', module_name='server_logs')
main_logger = Logger(log_file_name='main_logs.txt', module_name='main_logs')



