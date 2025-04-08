# === CREATE TABLES ===

CREATE_COUNTRY_TABLE = """
    CREATE TABLE IF NOT EXISTS country (
        id INTEGER PRIMARY KEY,
        code VARCHAR(2),
        name VARCHAR(50) NOT NULL UNIQUE
    )
"""

CREATE_IMPORT_LOG_TABLE = """
    CREATE TABLE IF NOT EXISTS import_log (
        id INTEGER PRIMARY KEY,
        batch_date TIMESTAMP,
        country_id INTEGER,
        import_directory_name VARCHAR(100),
        import_file_name VARCHAR(100),
        file_created_date TIMESTAMP,
        file_last_modified_date TIMESTAMP,
        row_count INTEGER,
        FOREIGN KEY (country_id) REFERENCES country(id)
    )
"""

CREATE_API_IMPORT_LOG_TABLE = """
    CREATE TABLE IF NOT EXISTS api_import_log (
        id INTEGER PRIMARY KEY,
        country_id INTEGER,
        api_id VARCHAR(20),
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        code_response INTEGER,
        error_messages TEXT,
        FOREIGN KEY (country_id) REFERENCES country(id)
    )
"""

CREATE_TRANSFORM_LOG_TABLE = """
    CREATE TABLE IF NOT EXISTS transform_log (
        id VARCHAR PRIMARY KEY,
        batch_date TIMESTAMP,
        country_id INTEGER,
        processed_directory_name VARCHAR,
        processed_file_name VARCHAR,
        row_count INTEGER,
        status VARCHAR
    )
"""

CREATE_WEATHER_DATA_IMPORT_TABLE = """
    CREATE TABLE IF NOT EXISTS weather_data_import (
        id VARCHAR PRIMARY KEY,
        country_id INTEGER,
        date DATE,
        tavg FLOAT,
        tmin FLOAT,
        tmax FLOAT,
        prcp FLOAT,
        snow FLOAT,
        wdir FLOAT,
        wspd FLOAT,
        wpgt FLOAT,
        pres FLOAT,
        tsun FLOAT
    )
"""

CREATE_COVID_19_DATA_IMPORT_TABLE = """
    CREATE TABLE IF NOT EXISTS covid_19_data_import (
        id VARCHAR PRIMARY KEY,
        country_id INTEGER,
        date DATE,
        cases INTEGER,
        deaths INTEGER,
        recovered INTEGER
    )
"""

CREATE_ETL_ERRORS_TABLE = """
    CREATE TABLE IF NOT EXISTS etl_errors (
        id VARCHAR PRIMARY KEY,
        error_code INTEGER,
        error_type VARCHAR,
        message VARCHAR,
        timestamp TIMESTAMP,
        severity VARCHAR,
        component VARCHAR,
        source_file VARCHAR,
        record_id VARCHAR,
        details VARCHAR
    )
"""

CREATE_TEMP_WEATHER_TABLE = """
    CREATE TEMP TABLE IF NOT EXISTS temp_weather_data (
        id VARCHAR,
        country_id VARCHAR,
        date VARCHAR,
        tavg FLOAT,
        tmin FLOAT,
        tmax FLOAT,
        prcp FLOAT,
        snow FLOAT,
        wdir FLOAT,
        wspd FLOAT,
        wpgt FLOAT,
        pres FLOAT,
        tsun FLOAT
    )
"""

CREATE_TEMP_COVID_TABLE = """
    CREATE TEMP TABLE IF NOT EXISTS temp_covid_data (
        id VARCHAR,
        country_id VARCHAR,
        date VARCHAR,
        cases INTEGER,
        deaths INTEGER,
        recovered INTEGER
    )
"""

DROP_TEMP_WEATHER_TABLE = "DROP TABLE IF EXISTS temp_weather_data"
DROP_TEMP_COVID_TABLE = "DROP TABLE IF EXISTS temp_covid_data"

# === INSERT DATA ===

INSERT_API_LOG = """
    INSERT INTO api_import_log 
    (id, country_id, api_id, start_time, end_time, code_response, error_messages)
    VALUES (?, ?, ?, ?, ?, ?, ?)
"""

INSERT_IMPORT_LOG = """
    INSERT INTO import_log
    (id, batch_date, country_id, import_directory_name, import_file_name, 
     file_created_date, file_last_modified_date, row_count)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""

INSERT_TRANSFORM_LOG = """
    INSERT INTO transform_log (
        id, batch_date, country_id, processed_directory_name, 
        processed_file_name, row_count, status
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
"""

INSERT_WEATHER_DATA = """
    INSERT INTO weather_data_import 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

INSERT_COVID_DATA = """
    INSERT INTO covid_19_data_import 
    VALUES (?, ?, ?, ?, ?, ?)
"""

INSERT_ETL_ERROR = """
    INSERT INTO etl_errors 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

INSERT_TEMP_WEATHER_DATA = """
    INSERT INTO temp_weather_data 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

INSERT_TEMP_COVID_DATA = """
    INSERT INTO temp_covid_data 
    VALUES (?, ?, ?, ?, ?, ?)
"""

# === SELECT QUERIES ===

GET_WEATHER__DATA_BY_DATE_RANGE = """
    SELECT * FROM weather_data_import 
    WHERE country_id = ? 
    AND date BETWEEN ? AND ?
    ORDER BY date
"""

GET_COVID_DATA_BY_DATE_RANGE = """
    SELECT * FROM covid_19_data_import 
    WHERE country_id = ? 
    AND date BETWEEN ? AND ?
    ORDER BY date
"""

GET_COUNTRY_BY_NAME = """
    SELECT id FROM country WHERE name = ?
"""

GET_MAX_API_LOG_ID = """
    SELECT COALESCE(MAX(id), 0) FROM api_import_log
"""

GET_MAX_IMPORT_LOG_ID = """
    SELECT COALESCE(MAX(id), 0) FROM import_log
"""

GET_ALL_TRANSFORM_LOGS = """
    SELECT * FROM transform_log 
    ORDER BY batch_date DESC
"""

GET_ALL_WEATHER_DATA = """
    SELECT * FROM weather_data_import 
    ORDER BY country_id, date
"""

GET_WEATHER_DATA_BY_COUNTRY = """
    SELECT * FROM weather_data_import 
    WHERE country_id = ? 
    ORDER BY date
"""

GET_ALL_COVID_DATA = """
    SELECT * FROM covid_19_data_import 
    ORDER BY country_id, date
"""

GET_COVID_DATA_BY_COUNTRY = """
    SELECT * FROM covid_19_data_import 
    WHERE country_id = ? 
    ORDER BY date
"""

# === TEMPLATE GENERATOR ===

CREATE_TEMP_TABLE = """
    CREATE TEMP TABLE IF NOT EXISTS {table_name} (
        {schema_sql}
    )
"""

DROP_TEMP_TABLE = "DROP TABLE IF EXISTS {table_name}"


# === LOG DATA SELECTS ===

# IMPORT LOG
GET_ALL_IMPORT_LOGS = """
    SELECT * FROM import_log ORDER BY batch_date DESC
"""

GET_IMPORT_LOGS_BY_DATE_RANGE = """
    SELECT * FROM import_log
    WHERE batch_date BETWEEN ? AND ?
    ORDER BY batch_date DESC
"""

GET_IMPORT_LOGS_BY_COUNTRY = """
    SELECT * FROM import_log
    WHERE country_id = ?
    ORDER BY batch_date DESC
"""

# API IMPORT LOG
GET_ALL_API_LOGS = """
    SELECT * FROM api_import_log ORDER BY start_time DESC
"""

GET_API_LOGS_BY_DATE_RANGE = """
    SELECT * FROM api_import_log
    WHERE start_time BETWEEN ? AND ?
    ORDER BY start_time DESC
"""

GET_API_LOGS_BY_COUNTRY = """
    SELECT * FROM api_import_log
    WHERE country_id = ?
    ORDER BY start_time DESC
"""

# TRANSFORM LOG
GET_TRANSFORM_LOGS_BY_DATE_RANGE = """
    SELECT * FROM transform_log
    WHERE batch_date BETWEEN ? AND ?
    ORDER BY batch_date DESC
"""

GET_TRANSFORM_LOGS_BY_COUNTRY = """
    SELECT * FROM transform_log
    WHERE country_id = ?
    ORDER BY batch_date DESC
"""
# REPORTING TABLES
CREATE_REPORTING_WEATHER_DATA = """
    CREATE TABLE IF NOT EXISTS reporting_weather_data (
        country_id INTEGER,
        date DATE,
        tavg FLOAT,
        prcp FLOAT,
        pres FLOAT,
        tsun FLOAT
    )
"""

CREATE_REPORTING_COVID_DATA = """
    CREATE TABLE IF NOT EXISTS reporting_covid_19_data (
        country_id INTEGER,
        date DATE,
        cases INTEGER
    )
"""

CREATE_REPORTING_API_LOG = """
    CREATE TABLE IF NOT EXISTS reporting_api_import_log AS
    SELECT * FROM api_import_log
"""

CREATE_REPORTING_IMPORT_LOG = """
    CREATE TABLE IF NOT EXISTS reporting_import_log AS
    SELECT * FROM import_log
"""

CREATE_REPORTING_TRANSFORM_LOG = """
    CREATE TABLE IF NOT EXISTS reporting_transform_log AS
    SELECT * FROM transform_log
"""

INSERT_REPORTING_WEATHER_DATA = """
    INSERT INTO reporting_weather_data (country_id, date, tavg, prcp, pres, tsun)
    SELECT country_id, date, tavg, prcp, pres, tsun FROM weather_data_import
"""

INSERT_REPORTING_COVID_DATA = """
    INSERT INTO reporting_covid_19_data (country_id, date, cases)
    SELECT country_id, date, cases FROM covid_19_data_import
"""

# === REPORTING DATA SELECTION ===
SELECT_DATA_FOR_REPORTING_WEATHER = """
    SELECT country_id, date, tavg, prcp, pres, tsun 
    FROM weather_data_import
"""

SELECT_DATA_FOR_REPORTING_COVID = """
    SELECT country_id, date, cases 
    FROM covid_19_data_import
"""

SELECT_DATA_FOR_REPORTING_API_LOG = """
    SELECT * FROM api_import_log
"""

SELECT_DATA_FOR_REPORTING_IMPORT_LOG = """
    SELECT * FROM import_log
"""

SELECT_DATA_FOR_REPORTING_TRANSFORM_LOG = """
    SELECT * FROM transform_log
"""

# === REPORTING DATA INSERTION ===
INSERT_DATA_INTO_REPORTING_WEATHER = """
    INSERT INTO reporting_weather_data (country_id, date, tavg, prcp, pres, tsun)
    VALUES (?, ?, ?, ?, ?, ?)
"""

INSERT_DATA_INTO_REPORTING_COVID = """
    INSERT INTO reporting_covid_19_data (country_id, date, cases)
    VALUES (?, ?, ?)
"""

INSERT_DATA_INTO_REPORTING_API_LOG = """
    INSERT INTO reporting_api_import_log VALUES (?, ?, ?, ?, ?, ?, ?)
"""

INSERT_DATA_INTO_REPORTING_IMPORT_LOG = """
    INSERT INTO reporting_import_log VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""

INSERT_DATA_INTO_REPORTING_TRANSFORM_LOG = """
    INSERT INTO reporting_transform_log VALUES (?, ?, ?, ?, ?, ?, ?)
"""

# === REPORTING TABLE INSERT FROM SELECT ===
INSERT_REPORTING_API_LOG = """
    INSERT INTO reporting_api_import_log SELECT * FROM api_import_log
"""

INSERT_REPORTING_IMPORT_LOG = """
    INSERT INTO reporting_import_log SELECT * FROM import_log
"""

INSERT_REPORTING_TRANSFORM_LOG = """
    INSERT INTO reporting_transform_log SELECT * FROM transform_log
"""
# === REPORTING TABLES QUERIES ===
GET_REPORTING_WEATHER_DATA = "SELECT * FROM reporting_weather_data"

GET_REPORTING_COVID_19_DATA = "SELECT * FROM reporting_covid_19_data"

GET_REPORTING_TRANSFORM_LOG = "SELECT * FROM reporting_transform_log"

GET_REPORTING_IMPORT_LOG = "SELECT * FROM reporting_import_log"

GET_REPORTING_API_IMPORT_LOG = "SELECT * FROM reporting_api_import_log"
