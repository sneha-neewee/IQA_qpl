from datetime import datetime
import logging
import logging.handlers as handlers
import inspect
import os

#===================FILE LOCATION=====================
current_filename = inspect.getframeinfo(inspect.currentframe()).filename
parent_dir_filename = os.path.dirname(os.path.abspath(current_filename))
parent_proj_dir = os.path.dirname(parent_dir_filename)
logs_path = os.path.join(parent_proj_dir, 'logs','')
file_name = 'log_details.log' # Log File Name

#==================LOGGER MANAGEMENT=================
formatter = logging.Formatter('%(asctime)s %(levelname)s [ %(name)s.%(funcName)s:%(lineno)d] - %(message)s')

def setup_logger(logger_name):

    # handler = handlers.TimedRotatingFileHandler(logs_path +'{}.log'.format(datetime.now().strftime("%Y-%m-%d")), when="midnight",interval=1, backupCount=14)
    handler = handlers.TimedRotatingFileHandler(logs_path +file_name, when="midnight",interval=1, backupCount=14)
    handler.suffix = '%Y-%m-%d_%H-%M-%S'
    handler.setFormatter(formatter)
    handler.setLevel(logging.INFO)     # Set logging level.

    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    return logger