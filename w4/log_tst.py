import logging
import sys
import os

CURRENT_FOLDER_NAME = os.path.dirname(os.path.abspath(__file__))


def setup_logger(module_name=None, level=logging.INFO, add_stdout_logger=True):
    custom_logger = logging.getLogger('global')
    if module_name:
        custom_logger = logging.getLogger(module_name)

    print("Clear all handlers in logger") # prevent multiple handler creation
    custom_logger.handlers.clear()

    if add_stdout_logger:
        print("Add stdout logger")
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(level)
        stdout_handler.setFormatter(logging.Formatter(fmt='%(asctime)-11s [%(levelname)s] [%(name)s] %(message)s'))
        custom_logger.addHandler(stdout_handler)

    f_handler = logging.FileHandler(os.path.join(CURRENT_FOLDER_NAME, 'logs', f'{module_name}.log'))
    custom_logger.addHandler(f_handler)

    custom_logger.setLevel(logging.DEBUG)

    return custom_logger


logger_module_1 = setup_logger(module_name='module1', level=logging.INFO)
logger_module_2 = setup_logger(module_name='module2', level=logging.DEBUG)

logger_module_1.debug("This is logger_module_1 log and will NOT be visible because it is setup to INFO log")

logger_module_2.debug("This is logger_module_2 log and will be visible because it is setup to DEBUG log")