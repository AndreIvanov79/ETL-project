import os
import glob
import json
import uuid
from datetime import datetime
from typing import Any
from data_validator import DataCleaner
from src.error_handling.error_handling import ErrorCode, ErrorSeverity

def transform_weather_batch(country: str, year_month: str, batch_date: datetime, db_manager: Any, error_manager: Any, logger: Any, validator: Any) -> int:
    from src.db import sql_templates

    total_processed = 0
    folder_path = os.path.join('..', 'extract', 'data', 'weather', country, year_month)

    db_manager.execute_query(sql_templates.CREATE_TEMP_WEATHER_TABLE)

    def log_transform_error(transform_id, country_id, directory_name, file_name, status):
        db_manager.insert_transform_log(
            transform_id=transform_id,
            batch_date=batch_date,
            country_id=country_id or country,
            directory_name=directory_name,
            file_name=file_name,
            row_count=0,
            status=status
        )

    if not os.path.exists(folder_path):
        transform_id = str(uuid.uuid4())
        country_id = db_manager.get_country_id(country)
        log_transform_error(transform_id, country_id, folder_path, "DIRECTORY", "NO_FILES_FOUND")
        return 0

    json_files = glob.glob(os.path.join(folder_path, '*.json'))
    if not json_files:
        transform_id = str(uuid.uuid4())
        country_id = db_manager.get_country_id(country)
        log_transform_error(transform_id, country_id, folder_path, "DIRECTORY", "EMPTY_DIRECTORY")
        return 0

    for file_path in sorted(json_files):
        file_name = os.path.basename(file_path)
        directory_name = os.path.dirname(file_path)
        transform_id = str(uuid.uuid4())

        try:
            with open(file_path, 'r') as f:
                try:
                    data = json.load(f)
                except json.JSONDecodeError as e:
                    country_id = db_manager.get_country_id(country)
                    log_transform_error(transform_id, country_id, directory_name, file_name, f"INVALID_JSON: {str(e)}")
                    continue

            # Определение даты
            if 'date' in data:
                date_str = data['date']
                try:
                    date_str = DataCleaner.normalize_date(date_str)
                except Exception as e:
                    country_id = db_manager.get_country_id(country)
                    log_transform_error(transform_id, country_id, directory_name, file_name, f"INVALID_DATE_FORMAT: {str(e)}")
                    continue
            else:
                try:
                    day = int(os.path.splitext(file_name)[0])
                    date_obj = datetime.strptime(f"{year_month}-{day:02d}", "%Y-%m-%d")
                    date_str = date_obj.strftime("%Y-%m-%d")
                except Exception as e:
                    country_id = db_manager.get_country_id(country)
                    log_transform_error(transform_id, country_id, directory_name, file_name, f"INVALID_DATE_FROM_FILENAME: {str(e)}")
                    continue

            clean_record = {
                "country_id": country,
                "date": date_str,
                "tavg": data.get("tavg"),
                "tmin": data.get("tmin"),
                "tmax": data.get("tmax"),
                "prcp": data.get("prcp"),
                "snow": data.get("snow"),
                "wdir": data.get("wdir"),
                "wspd": data.get("wspd"),
                "wpgt": data.get("wpgt"),
                "pres": data.get("pres"),
                "tsun": data.get("tsun")
            }

            errors = validator.validate("weather", clean_record)
            if errors:
                country_id = db_manager.get_country_id(country)
                log_transform_error(transform_id, country_id, directory_name, file_name, f"VALIDATION_ERROR: {'; '.join(errors)}")
                continue

            weather_id = str(uuid.uuid4())
            try:
                db_manager.insert_temp_weather_data(weather_id=weather_id, **clean_record)
            except Exception as e:
                country_id = db_manager.get_country_id(country)
                log_transform_error(transform_id, country_id, directory_name, file_name, f"DB_ERROR_TMP: {str(e)}")
                continue

            country_id = db_manager.get_country_id(clean_record["country_id"])
            if not country_id:
                log_transform_error(transform_id, None, directory_name, file_name, "COUNTRY_NOT_FOUND")
                continue

            try:
                db_manager.insert_weather_data(
                    weather_id=weather_id,
                    country_id=country_id,
                    **{k: v for k, v in clean_record.items() if k != 'country_id'}
                )
            except Exception as e:
                log_transform_error(transform_id, country_id, directory_name, file_name, f"DB_INSERT_ERROR: {str(e)}")
                continue

            db_manager.insert_transform_log(
                transform_id=transform_id,
                batch_date=batch_date,
                country_id=country_id,
                directory_name=directory_name,
                file_name=file_name,
                row_count=1,
                status="SUCCESS"
            )
            total_processed += 1

        except Exception as e:
            log_transform_error(str(uuid.uuid4()), None, directory_name, file_name, f"UNEXPECTED_ERROR: {str(e)}")
            continue

    try:
        db_manager.insert_transform_log(
            transform_id=str(uuid.uuid4()),
            batch_date=batch_date,
            country_id=db_manager.get_country_id(country),
            directory_name=folder_path,
            file_name="BATCH_PROCESS",
            row_count=total_processed,
            status="SUCCESS" if total_processed > 0 else "NO_RECORDS_PROCESSED"
        )
    except Exception as e:
        logger.error(f"Error logging final weather transform summary: {e}")

    db_manager.execute_query(sql_templates.DROP_TEMP_WEATHER_TABLE)

    return total_processed

def process_weather_complete_file(country: str, file_path: str, batch_date: datetime, db_manager: Any, error_manager: Any, logger: Any, validator: Any) -> int:
    transform_id = str(uuid.uuid4())
    total_records = 0

    def log_transform_error(transform_id, country_id, status):
        db_manager.insert_transform_log(
            transform_id=transform_id,
            batch_date=batch_date,
            country_id=country_id or country,
            directory_name=os.path.dirname(file_path),
            file_name=os.path.basename(file_path),
            row_count=0,
            status=status
        )

    try:
        with open(file_path, 'r') as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError as e:
                log_transform_error(transform_id, db_manager.get_country_id(country), f"INVALID_JSON: {str(e)}")
                return 0

        for record in data:
            clean_record = {
                "country_id": country,
                "date": record.get("date"),
                "tavg": record.get("tavg"),
                "tmin": record.get("tmin"),
                "tmax": record.get("tmax"),
                "prcp": record.get("prcp"),
                "snow": record.get("snow"),
                "wdir": record.get("wdir"),
                "wspd": record.get("wspd"),
                "wpgt": record.get("wpgt"),
                "pres": record.get("pres"),
                "tsun": record.get("tsun")
            }
            errors = validator.validate("weather", clean_record)
            if errors:
                log_transform_error(str(uuid.uuid4()), db_manager.get_country_id(country), f"VALIDATION_ERROR: {'; '.join(errors)}")
                continue

            weather_id = str(uuid.uuid4())
            try:
                db_manager.insert_temp_weather_data(weather_id=weather_id, **clean_record)
            except Exception as e:
                log_transform_error(str(uuid.uuid4()), db_manager.get_country_id(country), f"DB_ERROR_TMP: {str(e)}")
                continue

            country_id = db_manager.get_country_id(clean_record["country_id"])
            if not country_id:
                log_transform_error(str(uuid.uuid4()), None, "COUNTRY_NOT_FOUND")
                continue

            try:
                db_manager.insert_weather_data(weather_id=weather_id, country_id=country_id, **{k: v for k, v in clean_record.items() if k != 'country_id'})
            except Exception as e:
                log_transform_error(str(uuid.uuid4()), country_id, f"DB_INSERT_ERROR: {str(e)}")
                continue

            db_manager.insert_transform_log(
                transform_id=str(uuid.uuid4()),
                batch_date=batch_date,
                country_id=country_id,
                directory_name=os.path.dirname(file_path),
                file_name=os.path.basename(file_path),
                row_count=1,
                status="SUCCESS"
            )
            total_records += 1

        db_manager.insert_transform_log(
            transform_id=transform_id,
            batch_date=batch_date,
            country_id=db_manager.get_country_id(country),
            directory_name=os.path.dirname(file_path),
            file_name=os.path.basename(file_path),
            row_count=total_records,
            status="SUCCESS" if total_records > 0 else "NO_RECORDS_PROCESSED"
        )
        return total_records

    except Exception as e:
        db_manager.insert_transform_log(
            transform_id=transform_id,
            batch_date=batch_date,
            country_id=db_manager.get_country_id(country),
            directory_name=os.path.dirname(file_path),
            file_name=os.path.basename(file_path),
            row_count=0,
            status=f"UNEXPECTED_ERROR: {str(e)}"
        )
        return 0
