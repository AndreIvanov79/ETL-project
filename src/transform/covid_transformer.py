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

class CovidTransformer:
    def __init__(self, db_manager: DBManager, error_manager: ErrorManager, logger=None):
        self.db_manager = db_manager
        self.error_manager = error_manager
        self.logger = setup_logger()
        self.year_month = datetime(2022, datetime.now().month, datetime.now().day).strftime("%Y-%m")
        self._initialize_validators()

    def _initialize_validators(self):
        self.validator = SchemaValidator()
        covid_schema = {
            "country_id": [RequiredRule()],
            "date": [RequiredRule(), DateFormatRule()],
            "cases": [NumericRangeRule(min_val=0)],
            "deaths": [NumericRangeRule(min_val=0)],
            "recovered": [NumericRangeRule(min_val=0)]
        }
        self.validator.add_schema("covid", covid_schema)

    def transform(self, country: str, batch_date: datetime) -> int:
        from transform_utils.covid_transform import transform_covid_batch
        return transform_covid_batch(
            country=country,
            year_month=self.year_month,
            batch_date=batch_date,
            db_manager=self.db_manager,
            error_manager=self.error_manager,
            logger=self.logger,
            validator=self.validator
        )

    def _process_complete_file(self, country: str, file_path: str, batch_date: datetime) -> int:
        from transform_utils.covid_transform import process_covid_complete_file
        return process_covid_complete_file(
            country=country,
            file_path=file_path,
            batch_date=batch_date,
            db_manager=self.db_manager,
            error_manager=self.error_manager,
            logger=self.logger,
            validator=self.validator
        )
