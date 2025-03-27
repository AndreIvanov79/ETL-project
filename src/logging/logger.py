import logging
import os
from datetime import datetime

def setup_logger():
    os.makedirs('logs', exist_ok=True)
    
    log_filename = f'logs/etl_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger('etl_logger')
