import os
import json
import glob
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from data_validator import SchemaValidator, RequiredRule, DateFormatRule, NumericRangeRule, DataCleaner
from src.error_handling.error_handling import ErrorManager, ErrorCode, ErrorSeverity, ETLError
from src.db.db_manager import DBManager
from src.logging.logger import setup_logger
import uuid

class WeatherTransformer:
    def __init__(self, db_manager: DBManager, error_manager: ErrorManager, logger=None):
        self.db_manager = db_manager
        self.error_manager = error_manager
        self.logger = setup_logger()
        self.year_month = datetime(2022, datetime.now().month, datetime.now().day).strftime("%Y-%m")
        self._initialize_validators()

    def _initialize_validators(self):
        self.validator = SchemaValidator()
        weather_schema = {
            "country_id": [RequiredRule()],
            "date": [RequiredRule(), DateFormatRule()],
            "tavg": [NumericRangeRule(min_val=-100, max_val=100)],
            "tmin": [NumericRangeRule(min_val=-100, max_val=100)],
            "tmax": [NumericRangeRule(min_val=-100, max_val=100)],
            "prcp": [NumericRangeRule(min_val=0)],
            "snow": [NumericRangeRule(min_val=0)],
            "wdir": [NumericRangeRule(min_val=0, max_val=360)],
            "wspd": [NumericRangeRule(min_val=0)],
            "wpgt": [NumericRangeRule(min_val=0)],
            "pres": [NumericRangeRule(min_val=0)],
            "tsun": [NumericRangeRule(min_val=0)]
        }
        self.validator.add_schema("weather", weather_schema)

    def transform(self, country: str, batch_date: datetime) -> int:
        from transform_utils.weather_transform import transform_weather_batch
        return transform_weather_batch(
            country=country,
            year_month=self.year_month,
            batch_date=batch_date,
            db_manager=self.db_manager,
            error_manager=self.error_manager,
            logger=self.logger,
            validator=self.validator
        )

    def _process_complete_file(self, country: str, file_path: str, batch_date: datetime) -> int:
        from transform_utils.weather_transform import process_weather_complete_file
        return process_weather_complete_file(
            country=country,
            file_path=file_path,
            batch_date=batch_date,
            db_manager=self.db_manager,
            error_manager=self.error_manager,
            logger=self.logger,
            validator=self.validator
        )
    