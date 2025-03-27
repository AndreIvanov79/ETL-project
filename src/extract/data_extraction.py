import os
import requests
import json
import logging
from datetime import datetime


class DataExtractor:

    COUNTRY_COORDINATES = {
        'greece': {'lat': '37.98', 'lon': '23.73', 'alt': '43', 'city': 'athens'},
        'thailand': {'lat': '13.75', 'lon': '100.50', 'alt': '43', 'city': 'bangkok'},
        'mexico': {'lat': '19.43', 'lon': '-99.13', 'alt': '43', 'city': 'mexico_city'}
    }

    def __init__(self, countries=['greece', 'thailand', 'mexico']):
        self.countries = countries
        
        self.logger = self.setup_local_logger()
        
        self.rapidapi_key = os.getenv('RAPIDAPI_KEY', '')
        self.covid_api_key = os.getenv('COVID_API_KEY', '')
        
        self.create_directories()
    
    def setup_local_logger(self):
        """Set up local file logging"""
    
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
    
    def create_directories(self):
        """Create directories for data storage"""
        base_dirs = [
            f'data/weather/2020/{country}' for country in self.countries
        ] + [
            f'data/covid/2020/{country}' for country in self.countries
        ]
        
        for dir_path in base_dirs:
            os.makedirs(dir_path, exist_ok=True)
    
    def extract_weather_data(self):
        """Extract monthly weather data for specified countries using Meteostat API"""
        for country in self.countries:
            try:
                coords = self.COUNTRY_COORDINATES.get(country.lower())
                if not coords:
                    self.logger.error(f'No coordinates found for {country}')
                    continue

                url = "https://meteostat.p.rapidapi.com/point/monthly"
                
                querystring = {
                    "lat": coords['lat'],
                    "lon": coords['lon'],
                    "alt": coords['alt'],
                    "start": "2020-01-01",
                    "end": "2020-12-31"
                }

                headers = {
                    "x-rapidapi-key": self.rapidapi_key,
                    "x-rapidapi-host": "meteostat.p.rapidapi.com"
                }

                response = requests.get(url, headers=headers, params=querystring)
                
                if response.status_code == 200:
                    filename = f'data/weather/2020/{country}/weather_data_{coords["city"]}_{datetime.now().strftime("%Y%m%d")}.json'
                    with open(filename, 'w') as f:
                        json.dump(response.json(), f, indent=4)
                    
                    self.logger.info(f'Successfully extracted weather data for {country}')
                    self.log_extraction_details('weather', country, True)
                else:
                    self.logger.error(f'Failed to extract weather data for {country}: {response.text}')
                    self.log_extraction_details('weather', country, False, response.text)
            
            except Exception as e:
                self.logger.error(f'Error extracting weather data for {country}: {str(e)}')
                self.log_extraction_details('weather', country, False, str(e))
    
    def extract_covid_data(self):
        """Extract COVID-19 data for specified countries"""
        for country in self.countries:
            try:
                response = requests.get(
                    f'https://disease.sh/v3/covid-19/historical/{country}?strict=true&lastdays=366'
                )
                
                if response.status_code == 200:
                    filename = f'data/covid/2020/{country}/covid_data_{datetime.now().strftime("%Y%m%d")}.json'
                    with open(filename, 'w') as f:
                        json.dump(response.json(), f, indent=4)
                    
                    self.logger.info(f'Successfully extracted COVID-19 data for {country}')
                    self.log_extraction_details('covid', country, True)
                else:
                    self.logger.error(f'Failed to extract COVID-19 data for {country}: {response.text}')
                    self.log_extraction_details('covid', country, False, response.text)
            
            except Exception as e:
                self.logger.error(f'Error extracting COVID-19 data for {country}: {str(e)}')
                self.log_extraction_details('covid', country, False, str(e))
    
    def log_extraction_details(self, source, country, success, error_message=None):
        """Log extraction details to a separate log file"""

        os.makedirs('logs/extraction_details', exist_ok=True)
        
        log_filename = f'logs/extraction_details/{source}_extraction_log.csv'
        
        file_exists = os.path.exists(log_filename)
        
        with open(log_filename, 'a') as f:
            if not file_exists:
                f.write('timestamp,source,country,success,error_message\n')
            
            log_entry = (
                f"{datetime.now().isoformat()},"
                f"{source},"
                f"{country},"
                f"{success},"
                f"{error_message or ''}\n"
            )
            f.write(log_entry)
