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

        self.f_handler = logging.FileHandler(os.path.join(CURRENT_FOLDER_NAME, 'logs', log_file_name))

        # Create formatters and add it to handlers
        ######################################## YOUR CODE HERE ##################################################
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.f_handler.setFormatter(formatter)
        ######################################## YOUR CODE HERE ##################################################

        ######################################## YOUR CODE HERE ##################################################
        self.logger.addHandler(self.f_handler)
        self.logger.setLevel(logging.DEBUG)
        ######################################## YOUR CODE HERE ##################################################

    def warning(self, msg):
        pass
        ######################################## YOUR CODE HERE ##################################################
        self.logger.warning(msg)
        ######################################## YOUR CODE HERE ##################################################

    def error(self, msg):
        pass
        ######################################## YOUR CODE HERE ##################################################
        self.logger.error(msg)
        ######################################## YOUR CODE HERE ##################################################

    def info(self, msg):
        pass
        ######################################## YOUR CODE HERE ##################################################
        self.logger.info(msg)
        ######################################## YOUR CODE HERE ##################################################

    def debug(self, msg):
        pass
        ######################################## YOUR CODE HERE ##################################################
        self.logger.debug(msg)
        ######################################## YOUR CODE HERE ##################################################


server_logger = Logger(log_file_name='server_logs.txt', module_name='server_logs')
main_logger = Logger(log_file_name='main_logs.txt', module_name='main_logs')



