
CREATE_COUNTRY_TABLE = """
    CREATE TABLE IF NOT EXISTS country (
        id INTEGER PRIMARY KEY,
        code VARCHAR(2),
        name VARCHAR(50)
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
        country_id VARCHAR,
        processed_directory_name VARCHAR,
        processed_file_name VARCHAR,
        row_count INTEGER,
        status VARCHAR
    )
"""

CREATE_WEATHER_DATA_IMPORT_TABLE = """
    CREATE TABLE IF NOT EXISTS weather_data_import (
        id VARCHAR PRIMARY KEY,
        country_id VARCHAR,
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
        country_id VARCHAR,
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

# Data insertion templates
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
    INSERT INTO transform_log 
    VALUES (?, ?, ?, ?, ?, ?, ?)
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

# Query templates
GET_LATEST_WEATHER_DATA = """
    SELECT * FROM weather_data_import 
    WHERE country_id = ? 
    ORDER BY date DESC 
    LIMIT ?
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

# Additional templates for the DataTransformer class
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

CLEAR_TEMP_TABLE = """
    DELETE FROM {}
"""

INSERT_TEMP_WEATHER_DATA = """
    INSERT INTO temp_weather_data 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

INSERT_TEMP_COVID_DATA = """
    INSERT INTO temp_covid_data 
    VALUES (?, ?, ?, ?, ?, ?)
"""

GET_ALL_TRANSFORM_LOGS = """
    SELECT * 
    FROM transform_log 
    ORDER BY batch_date DESC
"""

GET_ALL_WEATHER_DATA = """
    SELECT * 
    FROM weather_data_import 
    ORDER BY country_id, date
"""

GET_WEATHER_DATA_BY_COUNTRY = """
    SELECT * 
    FROM weather_data_import 
    WHERE country_id = ? 
    ORDER BY date
"""

GET_ALL_COVID_DATA = """
    SELECT * 
    FROM covid_19_data_import 
    ORDER BY country_id, date
"""

GET_COVID_DATA_BY_COUNTRY = """
    SELECT * 
    FROM covid_19_data_import 
    WHERE country_id = ? 
    ORDER BY date
"""
