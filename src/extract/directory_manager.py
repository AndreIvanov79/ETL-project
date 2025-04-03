import os
from src.logging.logger import setup_logger
from datetime import datetime
from src.util.config import Config

class DirectoryManager:
    def __init__(self):
        self.logger = setup_logger()
    
    def create_directories(self, countries, data_types=['weather', 'covid']):
        for data_type in data_types:
            for country in countries:
                base_dir = f'{Config.DATA_DIR}/{data_type}/{country}'
                os.makedirs(base_dir, exist_ok=True)
                
                current_date = Config.START_DATE
                while current_date <= Config.END_DATE:
                    month_dir = f'{base_dir}/{current_date.strftime("%Y-%m")}'
                    os.makedirs(month_dir, exist_ok=True)
                    
                    if current_date.month == 12:
                        current_date = datetime(current_date.year + 1, 1, 1)
                    else:
                        current_date = datetime(current_date.year, current_date.month + 1, 1)
        
        self.logger.info(f"Created directory structure for {len(countries)} countries and {len(data_types)} data types")
