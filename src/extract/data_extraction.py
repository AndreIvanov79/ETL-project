import os
from dotenv import load_dotenv
import traceback
import requests
import json
import logging
from datetime import datetime, timedelta
import duckdb

load_dotenv()

class DataExtractor:
    
    COUNTRY_COORDINATES = {
        'greece': {'lat': '37.98', 'lon': '23.73', 'alt': '43', 'city': 'athens'},
        'thailand': {'lat': '13.75', 'lon': '100.50', 'alt': '43', 'city': 'bangkok'},
        'norway': {'lat': '59.91', 'lon': '10.75', 'alt': '23', 'city': 'oslo'}
    }
    
    COUNTRY_CODES = {
        'greece': 'GR',
        'thailand': 'TH',
        'norway': 'NO'
    }

    def __init__(self, countries=['greece', 'thailand', 'norway']):

        self.countries = countries
        
        self.logger = self.setup_local_logger()
        
        self.rapidapi_key = os.getenv('RAPIDAPI_KEY', '')
        
        current_date = datetime.now()
        self.target_date = datetime(2022, current_date.month, current_date.day)
        self.logger.info(f"Target date set to: {self.target_date.strftime('%Y-%m-%d')}")
        
        self.create_directories()
        
        self.initialize_db()
        
        self.api_log_id_counter = 1
        self.import_log_id_counter = 1
    
    def setup_local_logger(self):
        
        os.makedirs('logs', exist_ok=True)
        
        log_filename = f'logs/etl_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filename),
                logging.StreamHandler()
            ]
        )
        
        return logging.getLogger('etl_logger')
    
    def initialize_db(self):
        self.logger.info("Initializing DuckDB database")
        try:
            self.conn = duckdb.connect('../../etl_data.duckdb')
            
            self.conn.execute('''
                CREATE TABLE IF NOT EXISTS country (
                    id INTEGER PRIMARY KEY,
                    code VARCHAR(2),
                    name VARCHAR(50)
                )
            ''')
            
            self.conn.execute('''
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
            ''')
            
            self.conn.execute('''
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
            ''')
            
            for i, (country_name, code) in enumerate(self.COUNTRY_CODES.items(), 1):
                self.conn.execute(
                    "INSERT INTO country (id, code, name) VALUES (?, ?, ?) ON CONFLICT (id) DO NOTHING",
                    [i, code, country_name]
                )
            
            max_api_id = self.conn.execute("SELECT COALESCE(MAX(id), 0) FROM api_import_log").fetchone()[0]
            max_import_id = self.conn.execute("SELECT COALESCE(MAX(id), 0) FROM import_log").fetchone()[0]
            
            self.api_log_id_counter = max_api_id + 1
            self.import_log_id_counter = max_import_id + 1
            
            self.conn.commit()
            self.logger.info("Database initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Error initializing database: {str(e)}")
            traceback.print_exc()
    
    def get_country_id(self, country_name):
        result = self.conn.execute(
            "SELECT id FROM country WHERE name = ?",
            [country_name.lower()]
        ).fetchone()
        
        if result:
            return result[0]
        else:
            self.logger.error(f"Country {country_name} not found in database")
            return None
    
    def create_directories(self):
        for data_type in ['weather', 'covid']:
            for country in self.countries:
                year_dir = f'data/{data_type}/2022/{country}'
                os.makedirs(year_dir, exist_ok=True)
                
                month_dir = f'{year_dir}/{self.target_date.strftime("%Y-%m")}'
                os.makedirs(month_dir, exist_ok=True)
    
    def log_api_call(self, country, api_id, start_time, end_time, code_response, error_message=None):
        try:
            country_id = self.get_country_id(country)
            if country_id:
                current_id = self.api_log_id_counter
                self.api_log_id_counter += 1
                
                self.conn.execute('''
                    INSERT INTO api_import_log 
                    (id, country_id, api_id, start_time, end_time, code_response, error_messages)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', [
                    current_id,
                    country_id,
                    api_id,
                    start_time.isoformat(),
                    end_time.isoformat(),
                    code_response,
                    error_message
                ])
                self.conn.commit()
        except Exception as e:
            self.logger.error(f"Error logging API call: {str(e)}")
    
    def log_file_import(self, country, directory_name, file_name, row_count):
        try:
            country_id = self.get_country_id(country)
            if country_id:
                file_path = os.path.join(directory_name, file_name)
                if os.path.exists(file_path):
                    file_created = datetime.fromtimestamp(os.path.getctime(file_path))
                    file_modified = datetime.fromtimestamp(os.path.getmtime(file_path))
                else:
                    file_created = datetime.now()
                    file_modified = datetime.now()
                
                current_id = self.import_log_id_counter
                self.import_log_id_counter += 1
                
                self.conn.execute('''
                    INSERT INTO import_log
                    (id, batch_date, country_id, import_directory_name, import_file_name, 
                    file_created_date, file_last_modified_date, row_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', [
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
        except Exception as e:
            self.logger.error(f"Error logging file import: {str(e)}")
    
    def extract_weather_data(self):

        api_id = "meteostat"
        
        for country in self.countries:
            try:
                coords = self.COUNTRY_COORDINATES.get(country.lower())
                if not coords:
                    self.logger.error(f'No coordinates found for {country}')
                    continue
                
                url = "https://meteostat.p.rapidapi.com/point/daily"
                
                date_str = self.target_date.strftime("%Y-%m-%d")
                
                querystring = {
                    "lat": coords['lat'],
                    "lon": coords['lon'],
                    "alt": coords['alt'],
                    "start": date_str,
                    "end": date_str
                }

                headers = {
                    "x-rapidapi-key": self.rapidapi_key,
                    "x-rapidapi-host": "meteostat.p.rapidapi.com"
                }

                start_time = datetime.now()
                
                self.logger.info(f'Extracting weather data for {country} on {date_str}')
                response = requests.get(url, headers=headers, params=querystring)
                
                end_time = datetime.now()
                
                self.log_api_call(
                    country, 
                    api_id,
                    start_time,
                    end_time,
                    response.status_code,
                    None if response.status_code == 200 else response.text
                )
                
                if response.status_code == 200:

                    base_dir = f'data/weather/2022/{country}'
                    os.makedirs(base_dir, exist_ok=True)
                    
                    complete_filename = f'{base_dir}/weather_data_{date_str}_{coords["city"]}.json'
                    
                    with open(complete_filename, 'w') as f:
                        json.dump(response.json(), f, indent=4)
                    
                    self.logger.info(f'Successfully extracted weather data for {country} on {date_str}')
                    
                    month_folder = self.target_date.strftime('%Y-%m')
                    day_file = self.target_date.strftime('%d')
                    month_path = f'{base_dir}/{month_folder}'
                    os.makedirs(month_path, exist_ok=True)
                    day_path = f'{month_path}/{day_file}.json'
                    
                    if 'data' in response.json() and len(response.json()['data']) > 0:
                        with open(day_path, 'w') as f:
                            json.dump(response.json()['data'][0], f, indent=4)
                        
                        self.log_file_import(
                            country,
                            month_path,
                            f'{day_file}.json',
                            1 
                        )
                    else:
                        self.logger.warning(f'No weather data found for {country} on {date_str}')
                    
                    self.log_file_import(
                        country,
                        base_dir,
                        f'weather_data_{date_str}_{coords["city"]}.json',
                        len(response.json().get('data', []))
                    )
                else:
                    self.logger.error(f'Failed to extract weather data for {country}: {response.text}')
            
            except Exception as e:
                self.logger.error(f'Error extracting weather data for {country}: {str(e)}')
                error_time = datetime.now()
                self.log_api_call(
                    country,
                    api_id,
                    error_time,
                    error_time,
                    500,  
                    str(e)
                )
    
    def extract_covid_data(self):
        
        api_id = "disease.sh"
        
        for country in self.countries:
            try:
                country_code = self.COUNTRY_CODES.get(country.lower())
                if not country_code:
                    self.logger.error(f'No country code found for {country}')
                    continue

                current_date = f"{self.target_date.month}/{self.target_date.day}/{self.target_date.strftime('%y')}"

                url = f"https://disease.sh/v3/covid-19/historical/{country}?lastdays={current_date}"
                
                start_time = datetime.now()
                
                target_date_str = self.target_date.strftime('%Y-%m-%d')
                self.logger.info(f'Extracting COVID-19 data for {country} on {target_date_str}')
                response = requests.get(url)
                
                end_time = datetime.now()
                
                self.log_api_call(
                    country,
                    api_id,
                    start_time,
                    end_time,
                    response.status_code,
                    None if response.status_code == 200 else response.text
                )
                
                if response.status_code == 200:
                    
                    base_dir = f'data/covid/2022/{country}'
                    os.makedirs(base_dir, exist_ok=True)
                    
                    complete_filename = f'{base_dir}/covid_data_{target_date_str}.json'
                    
                    with open(complete_filename, 'w') as f:
                        json.dump(response.json(), f, indent=4)
                    
                    self.log_file_import(
                        country,
                        base_dir,
                        f'covid_data_{target_date_str}.json',
                        1  
                    )
                    
                    api_date_format = self.target_date.strftime('%-m/%-d/%y')
                    
                    timeline = None
                    if 'timeline' in response.json():
                        timeline = response.json()['timeline']
                    elif isinstance(response.json(), dict) and all(k in response.json() for k in ['cases', 'deaths', 'recovered']):
                        timeline = response.json()
                    
                    if timeline and 'cases' in timeline and api_date_format in timeline['cases']:
                        
                        daily_data = {
                            'date': target_date_str,
                            'cases': timeline['cases'].get(api_date_format, 0),
                            'deaths': timeline['deaths'].get(api_date_format, 0),
                            'recovered': timeline['recovered'].get(api_date_format, 0) if 'recovered' in timeline else None
                        }
                        
                        month_folder = self.target_date.strftime('%Y-%m')
                        day_file = self.target_date.strftime('%d')
                        month_path = f'{base_dir}/{month_folder}'
                        os.makedirs(month_path, exist_ok=True)
                        day_path = f'{month_path}/{day_file}.json'
                        
                        with open(day_path, 'w') as f:
                            json.dump(daily_data, f, indent=4)
                        
                        self.logger.info(f'Successfully extracted COVID-19 data for {country} on {target_date_str}')
                        
                        self.log_file_import(
                            country,
                            month_path,
                            f'{day_file}.json',
                            1  
                        )
                    else:
                        self.logger.warning(f'No COVID-19 data found for {country} on {target_date_str}')
                else:
                    self.logger.error(f'Failed to extract COVID-19 data for {country}: {response.text}')
            
            except Exception as e:
                self.logger.error(f'Error extracting COVID-19 data for {country}: {str(e)}')
                error_time = datetime.now()
                self.log_api_call(
                    country,
                    api_id,
                    error_time,
                    error_time,
                    500,  
                    str(e)
                )
    
    def close(self):
        if hasattr(self, 'conn'):
            self.conn.close()
            self.logger.info("Database connection closed")
