from datetime import datetime
import duckdb
import logging
import os
from sqlglot import parse_one, transpile
from sqlglot.errors import ParseError

from src.db import sql_templates
from src.util.config import Config


class DBManager:
    def __init__(self, db_path=None, logger=None):
        self.db_path = db_path or Config.DB_PATH
        self.logger = logger or logging.getLogger('etl_logger')
        self.conn = None
        self.api_log_id_counter = 1
        self.import_log_id_counter = 1
        self.connect()
        self._initialize_tables()

    def connect(self):
        try:
            self.logger.info(f"Connecting to DuckDB at {self.db_path}")
            self.conn = duckdb.connect(self.db_path)
            self.logger.info("Connected to DuckDB database")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {str(e)}")
            return False

    def execute_query(self, sql: str, params=None):
        try:
            transpiled_sql = transpile(sql, read='duckdb', write='duckdb')[0]
            if params:
                return self.conn.execute(transpiled_sql, params)
            else:
                return self.conn.execute(transpiled_sql)
        except Exception as e:
            self.logger.error(f"Error executing query: {e}")
            raise

    def _load_sql_template(self, template_name):
        try:
            sql = getattr(sql_templates, template_name)
            parse_one(sql)
            return sql
        except (AttributeError, ImportError) as e:
            self.logger.error(f"Error loading SQL template {template_name}: {e}")
            raise
        except ParseError as e:
            self.logger.error(f"SQL syntax error in {template_name}: {e}")
            raise

    def _initialize_tables(self):
        self.logger.info("Initializing database tables...")
        try:
            if not self.conn:
                if not self.connect():
                    return False

            table_templates = [
                "CREATE_COUNTRY_TABLE",
                "CREATE_IMPORT_LOG_TABLE",
                "CREATE_API_IMPORT_LOG_TABLE",
                "CREATE_TRANSFORM_LOG_TABLE",
                "CREATE_WEATHER_DATA_IMPORT_TABLE",
                "CREATE_COVID_19_DATA_IMPORT_TABLE",
                "CREATE_ETL_ERRORS_TABLE"
            ]

            for template_name in table_templates:
                sql = self._load_sql_template(template_name)
                self.execute_query(sql)

            for i, (country_name, code) in enumerate(Config.COUNTRY_CODES.items(), 1):
                self.execute_query(
                    "INSERT INTO country (id, code, name) VALUES (?, ?, ?) ON CONFLICT (id) DO NOTHING",
                    [i, code, country_name]
                )

            max_api_id = self.conn.execute("SELECT COALESCE(MAX(id), 0) FROM api_import_log").fetchone()[0]
            max_import_id = self.conn.execute("SELECT COALESCE(MAX(id), 0) FROM import_log").fetchone()[0]

            self.api_log_id_counter = max_api_id + 1
            self.import_log_id_counter = max_import_id + 1

            self.conn.commit()
            self.logger.info("Database initialized successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error initializing database: {str(e)}")
            return False

    def get_connection(self):
        if not self.conn:
            self.connect()
        return self.conn

    def get_country_id(self, country_name):
        if not self.conn:
            if not self.connect():
                return None

        result = self.execute_query(
            sql_templates.GET_COUNTRY_BY_NAME,
            [country_name.lower()]
        ).fetchone()

        if result:
            return result[0]
        else:
            self.logger.error(f"Country {country_name} not found in database")
            return None

    def log_api_call(self, country, api_id, start_time, end_time, code_response, error_message=None):
        try:
            if not self.conn:
                if not self.connect():
                    return False

            country_id = self.get_country_id(country)
            if country_id:
                current_id = self.api_log_id_counter
                self.api_log_id_counter += 1

                self.execute_query(sql_templates.INSERT_API_LOG, [
                    current_id,
                    country_id,
                    api_id,
                    start_time.isoformat(),
                    end_time.isoformat(),
                    code_response,
                    error_message
                ])
                self.conn.commit()
                return True
            return False
        except Exception as e:
            self.logger.error(f"Error logging API call: {str(e)}")
            return False

    def log_file_import(self, country, directory_name, file_name, row_count):
        try:
            if not self.conn:
                if not self.connect():
                    return False

            country_id = self.get_country_id(country)
            if country_id:
                file_path = os.path.join(directory_name, file_name)
                file_created = datetime.fromtimestamp(os.path.getctime(file_path)) if os.path.exists(file_path) else datetime.now()
                file_modified = datetime.fromtimestamp(os.path.getmtime(file_path)) if os.path.exists(file_path) else datetime.now()

                current_id = self.import_log_id_counter
                self.import_log_id_counter += 1

                self.execute_query(sql_templates.INSERT_IMPORT_LOG, [
                    current_id,
                    datetime.now().isoformat(),
                    country_id,
                    directory_name,
                    file_name,
                    file_created.isoformat(),
                    file_modified.isoformat(),
                    row_count
                ])
                self.conn.commit()
                return True
            return False
        except Exception as e:
            self.logger.error(f"Error logging file import: {str(e)}")
            return False

    def log_transform(self, transform_id, country, directory_name, file_name, row_count, status):
        try:
            if not self.conn:
                if not self.connect():
                    return False

            country_id = self.get_country_id(country)
            if country_id:
                self.execute_query(sql_templates.INSERT_TRANSFORM_LOG, [
                    transform_id,
                    datetime.now().isoformat(),
                    country_id,
                    directory_name,
                    file_name,
                    row_count,
                    status
                ])
                self.conn.commit()
                return True
            return False
        except Exception as e:
            self.logger.error(f"Error logging transform: {str(e)}")
            return False

    def insert_weather_data(self, weather_id, country_id, date, tavg, tmin, tmax, prcp, snow, wdir, wspd, wpgt, pres, tsun):
        try:
            if not self.conn:
                if not self.connect():
                    return False

            self.execute_query(sql_templates.INSERT_WEATHER_DATA, [
                weather_id, country_id, date, tavg, tmin, tmax, prcp, snow,
                wdir, wspd, wpgt, pres, tsun
            ])
            self.conn.commit()
            return True
        except Exception as e:
            self.logger.error(f"Error inserting weather data: {str(e)}")
            return False

    def insert_covid_data(self, covid_id, country_id, date, cases, deaths, recovered):
        try:
            if not self.conn:
                if not self.connect():
                    return False

            self.execute_query(sql_templates.INSERT_COVID_DATA, [
                covid_id, country_id, date, cases, deaths, recovered
            ])
            self.conn.commit()
            return True
        except Exception as e:
            self.logger.error(f"Error inserting COVID-19 data: {str(e)}")
            return False

    def log_etl_error(self, error_id, error_code, error_type, message, severity, component, source_file, record_id, details):
        try:
            if not self.conn:
                if not self.connect():
                    return False

            self.execute_query(sql_templates.INSERT_ETL_ERROR, [
                error_id,
                error_code,
                error_type,
                message,
                datetime.now().isoformat(),
                severity,
                component,
                source_file,
                record_id,
                details
            ])
            self.conn.commit()
            return True
        except Exception as e:
            self.logger.error(f"Error logging ETL error: {str(e)}")
            return False

    def get_latest_weather_data(self, country_id, limit=10):
        try:
            if not self.conn:
                if not self.connect():
                    return None

            result = self.execute_query(sql_templates.GET_LATEST_WEATHER_DATA, [country_id, limit]).fetchall()
            return result
        except Exception as e:
            self.logger.error(f"Error getting latest weather data: {str(e)}")
            return None

    def get_covid_data_by_date_range(self, country_id, start_date, end_date):
        try:
            if not self.conn:
                if not self.connect():
                    return None

            result = self.execute_query(sql_templates.GET_COVID_DATA_BY_DATE_RANGE, [country_id, start_date, end_date]).fetchall()
            return result
        except Exception as e:
            self.logger.error(f"Error getting COVID-19 data by date range: {str(e)}")
            return None

    def create_temp_table(self, table_name, schema_sql):
        try:
            create_sql = sql_templates.CREATE_TEMP_TABLE.format(table_name=table_name, schema_sql=schema_sql)
            self.execute_query(create_sql)
            self.logger.info(f"Created temporary table {table_name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to create temporary table {table_name}: {str(e)}")
            return False

    def drop_temp_table(self, table_name):
        try:
            drop_sql = sql_templates.DROP_TEMP_TABLE.format(table_name=table_name)
            self.execute_query(drop_sql)
            self.logger.info(f"Dropped temporary table {table_name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to drop temporary table {table_name}: {str(e)}")
            return False

    def insert_transform_log(self, transform_id, batch_date, country_id, directory_name, file_name, row_count, status):
        try:
            if not self.conn:
                if not self.connect():
                    return False

            self.execute_query(sql_templates.INSERT_TRANSFORM_LOG, [
                transform_id,
                batch_date.isoformat() if isinstance(batch_date, datetime) else batch_date,
                country_id,
                directory_name,
                file_name,
                row_count,
                status
            ])
            self.conn.commit()
            return True
        except Exception as e:
            self.logger.error(f"Failed to insert into transform_log table: {str(e)}")
            raise

    def insert_temp_covid_data(self, covid_id, country_id, date, cases, deaths, recovered):
        try:
            if not self.conn:
                if not self.connect():
                    return False

            self.execute_query(sql_templates.INSERT_TEMP_COVID_DATA, [
                covid_id,
                country_id,
                date,
                cases,
                deaths,
                recovered
            ])
            self.conn.commit()
            return True
        except Exception as e:
            self.logger.error(f"Failed to insert into temporary COVID data table: {str(e)}")
            raise

    def insert_temp_weather_data(self, weather_id, country_id, date, tavg, tmin, tmax, prcp, snow, wdir, wspd, wpgt, pres, tsun):
        try:
            if not self.conn:
                if not self.connect():
                    return False

            self.execute_query(sql_templates.INSERT_TEMP_WEATHER_DATA, [
                weather_id,
                country_id,
                date,
                tavg,
                tmin,
                tmax,
                prcp,
                snow,
                wdir,
                wspd,
                wpgt,
                pres,
                tsun
            ])
            self.conn.commit()
            return True
        except Exception as e:
            self.logger.error(f"Failed to insert into temporary weather data table: {str(e)}")
            raise

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
            self.logger.info("Database connection closed")
