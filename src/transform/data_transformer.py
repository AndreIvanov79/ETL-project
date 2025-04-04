import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
import json
from datetime import datetime
from src.logging.logger import setup_logger
from data_validator import SchemaValidator, RequiredRule, DateFormatRule, NumericRangeRule
from src.error_handling.error_handling import ErrorManager
from src.db.db_manager import DBManager
from covid_transformer import CovidTransformer
from weather_transformer import WeatherTransformer

class CommonDataTransformer:
    def __init__(self, db_path='../../etl_data.duckdb', logger=None):
        self.logger = setup_logger()
        self.db_manager = DBManager(db_path=db_path, logger=self.logger)
        self.conn = self.db_manager.get_connection()
        self.error_manager = ErrorManager(logger=self.logger, db_connection=self.conn)

        self.covid_transformer = CovidTransformer(
            db_manager=self.db_manager,
            error_manager=self.error_manager,
            logger=self.logger
        )

        self.weather_transformer = WeatherTransformer(
            db_manager=self.db_manager,
            error_manager=self.error_manager,
            logger=self.logger
        )

    def transform_all(self, countries=None):
        if countries is None:
            countries = ['greece', 'thailand', 'norway']

        batch_date = datetime.now()
        total_covid_records = 0
        total_weather_records = 0

        for country in countries:
            try:
                self.logger.info(f"Starting COVID transform for {country}")
                covid_count = self.covid_transformer.transform(country, batch_date)
                total_covid_records += covid_count or 0
            except Exception as e:
                self.logger.error(f"Error transforming COVID data for {country}: {e}")

            try:
                self.logger.info(f"Starting WEATHER transform for {country}")
                weather_count = self.weather_transformer.transform(country, batch_date)
                total_weather_records += weather_count
            except Exception as e:
                self.logger.error(f"Error transforming weather data for {country}: {e}")

        self.logger.info(f"Total COVID records processed: {total_covid_records}")
        self.logger.info(f"Total WEATHER records processed: {total_weather_records}")
        return {
            "covid": total_covid_records,
            "weather": total_weather_records
        }

    def close(self):
        self.db_manager.close()
        self.logger.info("Database connection closed.")

if __name__ == "__main__":
    transformer = CommonDataTransformer()
    try:
        results = transformer.transform_all()
        print("Transformation summary:", results)
    finally:
        transformer.close()
