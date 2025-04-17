from datetime import datetime
import duckdb
import os
from sqlglot import parse_one, transpile
from sqlglot.errors import ParseError

from src.db import sql_templates
from src.util.config import Config
from src.logging.logger import setup_logger


class DBManager:
    def __init__(self, db_path=None, logger=None):
        self.db_path = Config.DB_PATH 
        self.logger = setup_logger()
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
                    sql_templates.INSERT_COUNTRY,
                    [i, code, country_name]
                )

            max_api_id = self.conn.execute(sql_templates.GET_MAX_API_LOG_ID).fetchone()[0]
            max_import_id = self.conn.execute(sql_templates.GET_MAX_IMPORT_LOG_ID).fetchone()[0]

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

    def log_api_call(self, country_id, api_id, start_time, end_time, code_response, error_message=None):
        try:
            if not self.conn:
                if not self.connect():
                    return False

            country_id = country_id  
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

            country_id = country  
            if country_id:
                self.execute_query(sql_templates.INSERT_TRANSFORM_LOG, [
                    transform_id,
                    datetime.now().isoformat(),
                    country,
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

    def get_weather_data_by_date_range(self, country_id, start_date, end_date):
        try:
            if not self.conn:
                if not self.connect():
                    return None

            result = self.execute_query(sql_templates.GET_WEATHER__DATA_BY_DATE_RANGE, [country_id, start_date, end_date]).fetchall()
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


    def get_all_import_logs(self):
        return self.execute_query(sql_templates.GET_ALL_IMPORT_LOGS).fetchall()

    def get_import_logs_by_date_range(self, start_date, end_date):
        return self.execute_query(sql_templates.GET_IMPORT_LOGS_BY_DATE_RANGE, [start_date, end_date]).fetchall()

    def get_import_logs_by_country(self, country_id):
        return self.execute_query(sql_templates.GET_IMPORT_LOGS_BY_COUNTRY, [country_id]).fetchall()

    def get_all_api_logs(self):
        return self.execute_query(sql_templates.GET_ALL_API_LOGS).fetchall()

    def get_api_logs_by_date_range(self, start_date, end_date):
        return self.execute_query(sql_templates.GET_API_LOGS_BY_DATE_RANGE, [start_date, end_date]).fetchall()

    def get_api_logs_by_country(self, country_id):
        return self.execute_query(sql_templates.GET_API_LOGS_BY_COUNTRY, [country_id]).fetchall()

    def get_transform_logs_by_date_range(self, start_date, end_date):
        return self.execute_query(sql_templates.GET_TRANSFORM_LOGS_BY_DATE_RANGE, [start_date, end_date]).fetchall()

    def get_transform_logs_by_country(self, country_id):
        return self.execute_query(sql_templates.GET_TRANSFORM_LOGS_BY_COUNTRY, [country_id]).fetchall()

    def get_all_weather_data(self):
        return self.execute_query(sql_templates.GET_ALL_WEATHER_DATA).fetchall()

    def get_weather_data_by_country(self, country_id):
        return self.execute_query(sql_templates.GET_WEATHER_DATA_BY_COUNTRY, [country_id]).fetchall()

    def get_all_covid_data(self):
        return self.execute_query(sql_templates.GET_ALL_COVID_DATA).fetchall()

    def get_covid_data_by_country(self, country_id):
        return self.execute_query(sql_templates.GET_COVID_DATA_BY_COUNTRY, [country_id]).fetchall()

    def get_all_transform_logs(self):
        return self.execute_query(sql_templates.GET_ALL_TRANSFORM_LOGS).fetchall()

    def get_max_api_log_id(self):
        return self.execute_query(sql_templates.GET_MAX_API_LOG_ID).fetchone()[0]

    def get_max_import_log_id(self):
        return self.execute_query(sql_templates.GET_MAX_IMPORT_LOG_ID).fetchone()[0]
    
    def get_data_for_reporting_weather(self):
        try:
            if not self.conn:
                if not self.connect():
                    return []
            
            result = self.execute_query(sql_templates.SELECT_DATA_FOR_REPORTING_WEATHER).fetchall()
            return result
        except Exception as e:
            self.logger.error(f"Error getting data for reporting weather: {str(e)}")
            return []

    def get_data_for_reporting_covid(self):
        try:
            if not self.conn:
                if not self.connect():
                    return []
            
            result = self.execute_query(sql_templates.SELECT_DATA_FOR_REPORTING_COVID).fetchall()
            return result
        except Exception as e:
            self.logger.error(f"Error getting data for reporting COVID: {str(e)}")
            return []

    def get_data_for_reporting_logs(self, log_type):
        try:
            if not self.conn:
                if not self.connect():
                    return []
            
            if log_type == 'api':
                sql = sql_templates.SELECT_DATA_FOR_REPORTING_API_LOG
            elif log_type == 'import':
                sql = sql_templates.SELECT_DATA_FOR_REPORTING_IMPORT_LOG
            elif log_type == 'transform':
                sql = sql_templates.SELECT_DATA_FOR_REPORTING_TRANSFORM_LOG
            else:
                self.logger.error(f"Invalid log_type: {log_type}")
                return []
            
            result = self.execute_query(sql).fetchall()
            return result
        except Exception as e:
            self.logger.error(f"Error getting data for reporting {log_type} logs: {str(e)}")
            return []

    def insert_reporting_weather_data(self, country_id, date, tavg, prcp, pres, tsun):
        try:
            if not self.conn:
                if not self.connect():
                    return False
            
            self.execute_query(sql_templates.INSERT_DATA_INTO_REPORTING_WEATHER, [
                country_id, date, tavg, prcp, pres, tsun
            ])
            self.conn.commit()
            return True
        except Exception as e:
            self.logger.error(f"Error inserting into reporting weather data: {str(e)}")
            return False

    def insert_reporting_covid_data(self, country_id, date, cases):
        try:
            if not self.conn:
                if not self.connect():
                    return False
            
            self.execute_query(sql_templates.INSERT_DATA_INTO_REPORTING_COVID, [
                country_id, date, cases
            ])
            self.conn.commit()
            return True
        except Exception as e:
            self.logger.error(f"Error inserting into reporting COVID data: {str(e)}")
            return False

    def insert_reporting_api_log(self, id, country_id, api_id, start_time, end_time, code_response, error_messages):
        try:
            if not self.conn:
                if not self.connect():
                    return False
            
            self.execute_query(sql_templates.INSERT_DATA_INTO_REPORTING_API_LOG, [
                id, country_id, api_id, start_time, end_time, code_response, error_messages
            ])
            self.conn.commit()
            return True
        except Exception as e:
            self.logger.error(f"Error inserting into reporting API log: {str(e)}")
            return False

    def insert_reporting_import_log(self, id, batch_date, country_id, import_directory_name, import_file_name, file_created_date, file_last_modified_date, row_count):
        try:
            if not self.conn:
                if not self.connect():
                    return False
            
            self.execute_query(sql_templates.INSERT_DATA_INTO_REPORTING_IMPORT_LOG, [
                id, batch_date, country_id, import_directory_name, import_file_name, file_created_date, file_last_modified_date, row_count
            ])
            self.conn.commit()
            return True
        except Exception as e:
            self.logger.error(f"Error inserting into reporting import log: {str(e)}")
            return False

    def insert_reporting_transform_log(self, id, batch_date, country_id, processed_directory_name, processed_file_name, row_count, status):
        try:
            if not self.conn:
                if not self.connect():
                    return False
            
            self.execute_query(sql_templates.INSERT_DATA_INTO_REPORTING_TRANSFORM_LOG, [
                id, batch_date, country_id, processed_directory_name, processed_file_name, row_count, status
            ])
            self.conn.commit()
            return True
        except Exception as e:
            self.logger.error(f"Error inserting into reporting transform log: {str(e)}")
            return False

    def populate_reporting_tables(self):
        try:
            self.execute_query(sql_templates.CREATE_REPORTING_WEATHER_DATA)
            self.execute_query(sql_templates.CREATE_REPORTING_COVID_DATA)
            self.execute_query(sql_templates.CREATE_REPORTING_API_LOG)
            self.execute_query(sql_templates.CREATE_REPORTING_IMPORT_LOG)
            self.execute_query(sql_templates.CREATE_REPORTING_TRANSFORM_LOG)
            
            weather_data = self.get_data_for_reporting_weather()
            for record in weather_data:
                self.insert_reporting_weather_data(*record)
            
            covid_data = self.get_data_for_reporting_covid()
            for record in covid_data:
                self.insert_reporting_covid_data(*record)
            
            api_logs = self.get_data_for_reporting_logs('api')
            for record in api_logs:
                self.insert_reporting_api_log(*record)
            
            import_logs = self.get_data_for_reporting_logs('import')
            for record in import_logs:
                self.insert_reporting_import_log(*record)
            
            transform_logs = self.get_data_for_reporting_logs('transform')
            for record in transform_logs:
                self.insert_reporting_transform_log(*record)
            
            self.logger.info("All reporting tables populated successfully.")
            return True
        except Exception as e:
            self.logger.error(f"Failed to populate reporting tables: {e}")
            return False

    def refresh_reporting_tables(self):
        try:
            self.execute_query("TRUNCATE TABLE IF EXISTS reporting_weather_data")
            self.execute_query("TRUNCATE TABLE IF EXISTS reporting_covid_19_data")
            self.execute_query("TRUNCATE TABLE IF EXISTS reporting_api_import_log")
            self.execute_query("TRUNCATE TABLE IF EXISTS reporting_import_log")
            self.execute_query("TRUNCATE TABLE IF EXISTS reporting_transform_log")
            
            result = self.populate_reporting_tables()
            
            if result:
                self.logger.info("All reporting tables refreshed successfully.")
            
            return result
        except Exception as e:
            self.logger.error(f"Failed to refresh reporting tables: {e}")
            return False
        
    def prepare_reporting_tables(self):
        try:
            self.execute_query(sql_templates.CREATE_REPORTING_WEATHER_DATA)
            self.execute_query(sql_templates.CREATE_REPORTING_COVID_DATA)
            self.execute_query(sql_templates.CREATE_REPORTING_API_LOG)
            self.execute_query(sql_templates.CREATE_REPORTING_IMPORT_LOG)
            self.execute_query(sql_templates.CREATE_REPORTING_TRANSFORM_LOG)
            
            self.execute_query(sql_templates.INSERT_REPORTING_WEATHER_DATA)
            self.execute_query(sql_templates.INSERT_REPORTING_COVID_DATA)
            
            self.execute_query(sql_templates.INSERT_REPORTING_API_LOG)
            self.execute_query(sql_templates.INSERT_REPORTING_IMPORT_LOG)
            self.execute_query(sql_templates.INSERT_REPORTING_TRANSFORM_LOG)
            
            self.conn.commit()
            self.logger.info("Reporting tables created and filled successfully.")
            return True
        except Exception as e:
            self.logger.error(f"Failed to prepare reporting tables: {e}")
            return False
        
    def get_reporting_weather_data(self):
        try:
            if not self.conn:
                if not self.connect():
                    return []
            
            result = self.execute_query(sql_templates.GET_REPORTING_WEATHER_DATA).fetchall()
            return result
        except Exception as e:
            self.logger.error(f"Error retrieving data from reporting_weather_data: {str(e)}")
            return []

    def get_reporting_covid_data(self):
        try:
            if not self.conn:
                if not self.connect():
                    return []
            
            result = self.execute_query(sql_templates.GET_REPORTING_COVID_19_DATA).fetchall()
            return result
        except Exception as e:
            self.logger.error(f"Error retrieving data from reporting_covid_19_data: {str(e)}")
            return []

    def get_reporting_transform_log(self):
        try:
            if not self.conn:
                if not self.connect():
                    return []
            
            result = self.execute_query(sql_templates.GET_REPORTING_TRANSFORM_LOG).fetchall()
            return result
        except Exception as e:
            self.logger.error(f"Error retrieving data from reporting_transform_log: {str(e)}")
            return []

    def get_reporting_import_log(self):
        try:
            if not self.conn:
                if not self.connect():
                    return []
            
            result = self.execute_query(sql_templates.GET_REPORTING_IMPORT_LOG).fetchall()
            return result
        except Exception as e:
            self.logger.error(f"Error retrieving data from reporting_import_log: {str(e)}")
            return []

    def get_reporting_api_import_log(self):
        try:
            if not self.conn:
                if not self.connect():
                    return []
            
            result = self.execute_query(sql_templates.GET_REPORTING_API_IMPORT_LOG).fetchall()
            return result
        except Exception as e:
            self.logger.error(f"Error retrieving data from reporting_api_import_log: {str(e)}")
            return []
        
    def get_covid_weather_for_training(self):
        return self.execute_query(sql_templates.GET_COVID_WEATHER_FOR_TRAINING).fetchdf()


    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
            self.logger.info("Database connection closed")
