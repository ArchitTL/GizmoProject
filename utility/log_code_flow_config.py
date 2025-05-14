import logging
from variables.variables import log_code_flow_file_name
LOG_FILE = log_code_flow_file_name

def setup_logger():
    logging.basicConfig(
        filename=LOG_FILE,
        filemode="a",
        format="%(asctime)s - %(levelname)s - %(message)s",
        level=logging.INFO
    )
    return logging.getLogger()  # return root logger
