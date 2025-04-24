import os
import glob
import json
import uuid
from datetime import datetime
from typing import Any
from src.transform.data_validator import DataCleaner
from src.error_handling.error_handling import ErrorCode, ErrorSeverity
from src.db import sql_templates
from src.logging.logger import setup_logger


def transform_weather_batch(
    country: str,
    year_month: str,
    batch_date: datetime,
    db_manager: Any,
    error_manager: Any,
    logger: Any,
    validator: Any
) -> int:
    total_processed = 0
    folder_path = os.path.join(
        'src', 'extract', 'data', 'weather', country, year_month
    )

    db_manager.execute_query(sql_templates.CREATE_TEMP_WEATHER_TABLE)

    def log_transform_error(tid, cid, dname, fname, status):
        db_manager.insert_transform_log(
            transform_id=tid,
            batch_date=batch_date,
            country_id=cid or country,
            directory_name=dname,
            file_name=fname,
            row_count=0,
            status=status
        )

    if not os.path.isdir(folder_path):
        tid = str(uuid.uuid4())
        cid = db_manager.get_country_id(country)
        log_transform_error(tid, cid, folder_path, 'DIRECTORY', 'NO_FILES_FOUND')
        return 0

    json_files = glob.glob(os.path.join(folder_path, '*.json'))
    if not json_files:
        tid = str(uuid.uuid4())
        cid = db_manager.get_country_id(country)
        log_transform_error(tid, cid, folder_path, 'DIRECTORY', 'EMPTY_DIRECTORY')
        return 0

    monthly_file = os.path.join(folder_path, f"{year_month}.json")
    if monthly_file in json_files:
        from src.transform.transform_utils.weather_transform import process_weather_complete_file
        return process_weather_complete_file(
            country=country,
            file_path=monthly_file,
            batch_date=batch_date,
            db_manager=db_manager,
            error_manager=error_manager,
            logger=logger,
            validator=validator
        )

    for file_path in sorted(json_files):
        fname = os.path.basename(file_path)
        dname = os.path.dirname(file_path)
        tid = str(uuid.uuid4())
        try:
            with open(file_path, 'r') as f:
                try:
                    data = json.load(f)
                except json.JSONDecodeError as e:
                    cid = db_manager.get_country_id(country)
                    log_transform_error(tid, cid, dname, fname, f"INVALID_JSON: {e}")
                    continue

            if isinstance(data, dict) and 'date' in data:
                date_str = data['date']
                try:
                    date_str = DataCleaner.normalize_date(date_str)
                except Exception as e:
                    cid = db_manager.get_country_id(country)
                    log_transform_error(tid, cid, dname, fname, f"INVALID_DATE_FORMAT: {e}")
                    continue
            else:
                try:
                    day = int(os.path.splitext(fname)[0])
                    dt = datetime.strptime(f"{year_month}-{day:02d}", "%Y-%m-%d")
                    date_str = dt.strftime("%Y-%m-%d")
                except Exception as e:
                    cid = db_manager.get_country_id(country)
                    log_transform_error(tid, cid, dname, fname, f"INVALID_DATE_FROM_FILENAME: {e}")
                    continue

            rec = {
                'country_id': country,
                'date': date_str,
                'tavg': data.get('tavg'),
                'tmin': data.get('tmin'),
                'tmax': data.get('tmax'),
                'prcp': data.get('prcp'),
                'snow': data.get('snow'),
                'wdir': data.get('wdir'),
                'wspd': data.get('wspd'),
                'wpgt': data.get('wpgt'),
                'pres': data.get('pres'),
                'tsun': data.get('tsun')
            }

            errors = validator.validate('weather', rec)
            if errors:
                cid = db_manager.get_country_id(country)
                log_transform_error(tid, cid, dname, fname, f"VALIDATION_ERROR: {';'.join(errors)}")
                continue

            wid = str(uuid.uuid4())
            try:
                db_manager.insert_temp_weather_data(weather_id=wid, **rec)
            except Exception as e:
                cid = db_manager.get_country_id(country)
                log_transform_error(tid, cid, dname, fname, f"DB_ERROR_TMP: {e}")
                continue

            cid = db_manager.get_country_id(country)
            if not cid:
                log_transform_error(tid, None, dname, fname, 'COUNTRY_NOT_FOUND')
                continue

            try:
                db_manager.insert_weather_data(
                    weather_id=wid,
                    country_id=cid,
                    date=rec['date'],
                    tavg=rec['tavg'],
                    tmin=rec['tmin'],
                    tmax=rec['tmax'],
                    prcp=rec['prcp'],
                    snow=rec['snow'],
                    wdir=rec['wdir'],
                    wspd=rec['wspd'],
                    wpgt=rec['wpgt'],
                    pres=rec['pres'],
                    tsun=rec['tsun']
                )
            except Exception as e:
                log_transform_error(tid, cid, dname, fname, f"DB_INSERT_ERROR: {e}")
                continue

            db_manager.insert_transform_log(
                transform_id=tid,
                batch_date=batch_date,
                country_id=cid,
                directory_name=dname,
                file_name=fname,
                row_count=1,
                status='SUCCESS'
            )
            total_processed += 1

        except Exception as e:
            log_transform_error(str(uuid.uuid4()), None, dname, fname, f"UNEXPECTED_ERROR: {e}")

    try:
        db_manager.insert_transform_log(
            transform_id=str(uuid.uuid4()),
            batch_date=batch_date,
            country_id=db_manager.get_country_id(country),
            directory_name=folder_path,
            file_name='BATCH_PROCESS',
            row_count=total_processed,
            status='SUCCESS' if total_processed else 'NO_RECORDS_PROCESSED'
        )
    except Exception as e:
        logger.error(f"Error logging weather transform summary: {e}")

    db_manager.execute_query(sql_templates.DROP_TEMP_WEATHER_TABLE)
    return total_processed


def process_weather_complete_file(
    country: str,
    file_path: str,
    batch_date: datetime,
    db_manager: Any,
    error_manager: Any,
    logger: Any,
    validator: Any
) -> int:
    tid = str(uuid.uuid4())
    total = 0
    def log_transform_error(tid_inner, cid_inner, status):
        db_manager.insert_transform_log(
            transform_id=tid_inner,
            batch_date=batch_date,
            country_id=cid_inner or country,
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
                log_transform_error(tid, db_manager.get_country_id(country), f"INVALID_JSON: {e}")
                return 0

        for record in data:
            rec = {
                'country_id': country,
                'date': record.get('date'),
                'tavg': record.get('tavg'),
                'tmin': record.get('tmin'),
                'tmax': record.get('tmax'),
                'prcp': record.get('prcp'),
                'snow': record.get('snow'),
                'wdir': record.get('wdir'),
                'wspd': record.get('wspd'),
                'wpgt': record.get('wpgt'),
                'pres': record.get('pres'),
                'tsun': record.get('tsun')
            }
            errors = validator.validate('weather', rec)
            if errors:
                log_transform_error(str(uuid.uuid4()), db_manager.get_country_id(country), f"VALIDATION_ERROR: {';'.join(errors)}")
                continue
            wid = str(uuid.uuid4())
            try:
                db_manager.insert_temp_weather_data(weather_id=wid, **rec)
            except Exception as e:
                log_transform_error(str(uuid.uuid4()), db_manager.get_country_id(country), f"DB_ERROR_TMP: {e}")
                continue
            cid = db_manager.get_country_id(country)
            if not cid:
                log_transform_error(str(uuid.uuid4()), None, 'COUNTRY_NOT_FOUND')
                continue
            try:
                db_manager.insert_weather_data(weather_id=wid, country_id=cid,
                                               date=rec['date'], tavg=rec['tavg'],
                                               tmin=rec['tmin'], tmax=rec['tmax'],
                                               prcp=rec['prcp'], snow=rec['snow'],
                                               wdir=rec['wdir'], wspd=rec['wspd'],
                                               wpgt=rec['wpgt'], pres=rec['pres'], tsun=rec['tsun'])
            except Exception as e:
                log_transform_error(str(uuid.uuid4()), cid, f"DB_INSERT_ERROR: {e}")
                continue
            db_manager.insert_transform_log(
                transform_id=str(uuid.uuid4()),
                batch_date=batch_date,
                country_id=cid,
                directory_name=os.path.dirname(file_path),
                file_name=os.path.basename(file_path),
                row_count=1,
                status='SUCCESS'
            )
            total += 1
        db_manager.insert_transform_log(
            transform_id=tid,
            batch_date=batch_date,
            country_id=db_manager.get_country_id(country),
            directory_name=os.path.dirname(file_path),
            file_name=os.path.basename(file_path),
            row_count=total,
            status='SUCCESS' if total else 'NO_RECORDS_PROCESSED'
        )
        return total
    except Exception as e:
        db_manager.insert_transform_log(
            transform_id=tid,
            batch_date=batch_date,
            country_id=db_manager.get_country_id(country),
            directory_name=os.path.dirname(file_path),
            file_name=os.path.basename(file_path),
            row_count=0,
            status=f"UNEXPECTED_ERROR: {e}"
        )
        return 0
