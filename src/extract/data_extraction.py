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
from src.error_handling.error_handling import ErrorManager, ErrorSeverity, ErrorCode, ETLError


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
        
        self.error_manager = ErrorManager(logger=self.logger, db_connection=self.db_manager.get_connection())
    
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
            self.error_manager.create_error(
                code=ErrorCode.UNKNOWN_ERROR,
                message=f"Unexpected error in data extraction process: {str(e)}",
                severity=ErrorSeverity.CRITICAL,
                component="DataExtraction.extract_all_data",
                details={"exception": str(e)}
            )
            traceback.print_exc()
            return False
        finally:
            self.close()
    
    def close(self):
        if hasattr(self, 'db_manager'):
            self.db_manager.close()
    
    def log_error(self, code: ErrorCode, message: str, severity: ErrorSeverity, 
                  component: str, source_file: str = None, record_id: str = None, 
                  details: dict = None):
        """Log an error using the ErrorManager"""
        self.error_manager.create_error(
            code=code,
            message=message,
            severity=severity,
            component=component,
            source_file=source_file,
            record_id=record_id,
            details=details
        )

if __name__ == "__main__":
    extractor = DataExtraction()
    extractor.extract_all_data()
