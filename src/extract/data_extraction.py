import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
import traceback
from src.util.config import Config
from src.logging.logger import setup_logger
from src.db.db_manager import DBManager
from directory_manager import DirectoryManager
from api_client import ApiClient
from data_processors import WeatherDataProcessor, CovidDataProcessor
from extractors import WeatherExtractor, CovidExtractor


class DataExtraction:
    def __init__(self, countries=None):
        self.logger = setup_logger()
        
        self.countries = countries or list(Config.COUNTRY_CODES.keys())
        
        self.db_manager = DBManager(logger=self.logger)
        if not self.db_manager.get_connection():
            raise Exception("Failed to initialize database")
        
        self.dir_manager = DirectoryManager()
        self.dir_manager.create_directories(self.countries)
        
        self.api_client = ApiClient(self.db_manager)
        
        self.weather_processor = WeatherDataProcessor(self.db_manager)
        self.covid_processor = CovidDataProcessor(self.db_manager)
        
        self.weather_extractor = WeatherExtractor(self.api_client, self.weather_processor, self.db_manager)
        self.covid_extractor = CovidExtractor(self.api_client, self.covid_processor, self.db_manager)
    
    def extract_all_data(self):
        try:
            self.logger.info(f"Starting data extraction for {len(self.countries)} countries")
            
            weather_success = self.weather_extractor.extract_single_day_data(self.countries)
            
            covid_success = self.covid_extractor.extract_single_day_data(self.countries)
            
            if weather_success and covid_success:
                self.logger.info("Data extraction completed successfully for all countries and data types")
                return True
            else:
                self.logger.warning("Data extraction completed with some failures")
                return False
                
        except Exception as e:
            self.logger.error(f"Error in data extraction process: {str(e)}")
            traceback.print_exc()
            return False
        finally:
            self.close()
    
    def close(self):
        if hasattr(self, 'db_manager'):
            self.db_manager.close()

if __name__ == "__main__":
    extractor = DataExtraction()
    extractor.extract_all_data()
