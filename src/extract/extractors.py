from datetime import datetime, timedelta
import json
import requests
from src.logging.logger import setup_logger
import os
from src.util.config import Config
from concurrent.futures import ThreadPoolExecutor, as_completed

class WeatherExtractor:

    @staticmethod
    def _date_range(start, end):
        current = start
        while current <= end:
            yield current
            current += timedelta(days=1)

    def __init__(self, api_client, data_processor, db):
        self.logger = setup_logger()
        self.api_client = api_client
        self.data_processor = data_processor
        self.db = db
    
    def extract_for_country(self, country):
        try:
            coords = Config.COUNTRY_COORDINATES.get(country.lower())
            if not coords:
                self.logger.error(f'No coordinates found for {country}')
                return False
            country_id = self.db.get_country_id(country)
            
            url = f"https://archive-api.open-meteo.com/v1/archive?latitude={coords["lat"]}&longitude={coords["lon"]}&start_date=2020-01-01&end_date=2021-01-01&daily=temperature_2m_mean,temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max,shortwave_radiation_sum"
        
            self.logger.info(f'Extracting weather data for {country} from {Config.START_DATE} to {Config.END_DATE}')
            start_time = datetime.now()
          
            response, success = self.api_client.make_request(
                "meteostat",
                country,
                url
            )
            end_time = datetime.now()
            print("Status code:", response.status_code)
            print("Response text:", response.text[:300])
            if success:
                json_data, dir_name, file_name = self.data_processor.save_response(
                    country,
                    'weather',
                    response,
                    f'weather_data_complete_{coords["city"]}.json'
                )
                
                if json_data:
                    row_count = self.data_processor.split_daily_data(country, json_data, Config.START_DATE, Config.END_DATE)
                    
                    self.db.log_file_import(
                        country_id,
                        dir_name,
                        file_name,
                        row_count
                    )

                    self.db.log_api_call(
                        country_id,
                        "meteostat",
                        start_time,
                        end_time,
                        response.status_code if hasattr(response, 'status_code') else 500,
                        None if success else str(response)
                    )
                    
                    return True
            
            return False
        
        except Exception as e:
            self.logger.error(f'Unexpected error extracting weather data for {country}: {str(e)}')
            return False
    
    def extract_data(self, countries):
        success_count = 0
        total_count = len(countries)
        
        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            futures = {executor.submit(self.extract_for_country, country): country for country in countries}
            
            for future in as_completed(futures):
                country = futures[future]
                try:
                    result = future.result()
                    if result:
                        success_count += 1
                        self.logger.info(f"Weather data extraction for {country} completed successfully")
                    else:
                        self.logger.error(f"Weather data extraction for {country} failed")
                except Exception as e:
                    self.logger.error(f"Error processing weather data for {country}: {str(e)}")
        
        self.logger.info(f"Weather data extraction completed: {success_count}/{total_count} successful")
        return success_count == total_count
        
    def extract_single_day_for_country(self, country, specific_date=None):
        try:
            coords = Config.COUNTRY_COORDINATES.get(country.lower())
            if not coords:
                self.logger.error(f'No coordinates found for {country}')
                return False

            country_id = self.db.get_country_id(country)
            target_date = specific_date or Config.START_DATE
            start_date_str = Config.START_DATE.strftime("%Y-%m-%d")
            end_date_str = Config.START_DATE.strftime("%Y-%m-%d")

            url = f"https://archive-api.open-meteo.com/v1/archive?latitude={coords['lat']}&longitude={coords['lon']}&start_date={start_date_str}&end_date={end_date_str}&daily=temperature_2m_mean,temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max,shortwave_radiation_sum"

            self.logger.info(f'Extracting weather data for {country} for {start_date_str}')
            start_time = datetime.now()
            response, success = self.api_client.make_request("meteostat", country, url)
            end_time = datetime.now()

            self.db.log_api_call(
                country_id,
                "meteostat",
                start_time,
                end_time,
                response.status_code if hasattr(response, 'status_code') else 500,
                None if success else str(response)
            )

            if not success:
                return False

            json_data = response.json()
            row_count = self.data_processor.split_daily_data(country, json_data, target_date, target_date)
            return row_count > 0

        except Exception as e:
            self.logger.error(f'Error extracting weather data for {country}: {str(e)}')
            return False

        
    def extract_single_day_data(self, countries, specific_date=None, start_date=None, end_date=None):
        success_count = 0
        total_count = len(countries)

        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            futures = {}

            for country in countries:
                if specific_date:
                    futures[executor.submit(self.extract_single_day_for_country, country, specific_date)] = country
                elif start_date and end_date:
                    for date in self._date_range(start_date, end_date):
                        futures[executor.submit(self.extract_single_day_for_country, country, date)] = f"{country}_{date.strftime('%Y-%m-%d')}"
                else:
                    futures[executor.submit(self.extract_single_day_for_country, country)] = country

            for future in as_completed(futures):
                label = futures[future]
                try:
                    result = future.result()
                    if result:
                        success_count += 1
                        self.logger.info(f"Weather data extraction for {label} completed successfully")
                    else:
                        self.logger.error(f"Weather data extraction for {label} failed")
                except Exception as e:
                    self.logger.error(f"Error processing weather data for {label}: {str(e)}")

        self.logger.info(f"Single day weather data extraction completed: {success_count}/{total_count} successful")
        return success_count == total_count

class CovidExtractor:

    @staticmethod
    def _date_range(start, end):
        current = start
        while current <= end:
            yield current
            current += timedelta(days=1)

    def __init__(self, api_client, data_processor, db):
        self.logger = setup_logger()
        self.api_client = api_client
        self.data_processor = data_processor
        self.db = db

    def extract_for_country(self, country):
        try:
            country_code = Config.COUNTRY_CODES.get(country.lower())
            if not country_code:
                self.logger.error(f'No data found for {country}')
                return False
            
            country_id = self.db.get_country_id(country) 
            url = f"https://storage.googleapis.com/covid19-open-data/v3/location/{country_code}.json"
            headers = {}

            self.logger.info(f'Extracting COVID-19 data for {country} from {Config.START_DATE} to {Config.END_DATE}')
            start_time = datetime.now()
            response, success = self.api_client.make_request(
                "covid-19",
                country,
                url,
                headers=headers,
            )
            end_time = datetime.now()
                
            if success:
                raw_data = response.json()
                normalized_data = self.normalize_json_data(raw_data)

                row_count = self.data_processor.split_daily_data(
                    country,
                    normalized_data,
                    Config.START_DATE,
                    Config.END_DATE
                )

                self.db.log_api_call(
                    country_id,
                    "covid",
                    start_time,
                    end_time,
                    response.status_code if hasattr(response, 'status_code') else 500,
                    None if success else str(response)
                )

                return row_count > 0

            return False
            
        except Exception as e:
            self.logger.error(f'Unexpected error extracting COVID-19 data for {country}: {str(e)}')
            return False

    def normalize_json_data(self, json_data):
        columns = json_data.get('columns', [])
        data_rows = json_data.get('data', [])
        
        normalized_data = []
        
        for row in data_rows:
            row_dict = {columns[i]: value for i, value in enumerate(row)}
            normalized_data.append(row_dict)
        
        return normalized_data
    
    def extract_data(self, countries):
        success_count = 0
        total_count = len(countries)
        
        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            futures = {executor.submit(self.extract_for_country, country): country for country in countries}
            
            for future in as_completed(futures):
                country = futures[future]
                try:
                    result = future.result()
                    if result:
                        success_count += 1
                        self.logger.info(f"Covid-19 data extraction for {country} completed successfully")
                    else:
                        self.logger.error(f"Covid-19 data extraction for {country} failed")
                except Exception as e:
                    self.logger.error(f"Error processing covid-19 data for {country}: {str(e)}")
        
        self.logger.info(f"Covid-19 data extraction completed: {success_count}/{total_count} successful")
        return success_count == total_count
    

    def extract_single_day_for_country(self, country, specific_date=None):
        try:
            country_code = Config.COUNTRY_CODES.get(country.lower())
            if not country_code:
                self.logger.error(f'No country code found for {country}')
                return False

            country_id = self.db.get_country_id(country)
            target_date = specific_date or datetime.now()
            target_date = datetime(2022, target_date.month, target_date.day)
            target_date_str = target_date.strftime('%Y-%m-%d')

            url = f"https://storage.googleapis.com/covid19-open-data/v3/location/{country_code}.json"
            headers = {}

            self.logger.info(f'Extracting COVID-19 data for {country} for {target_date_str}')
            start_time = datetime.now()
            response, success = self.api_client.make_request("covid", country, url, headers=headers)
            end_time = datetime.now()

            self.db.log_api_call(
                country_id,
                "covid",
                start_time,
                end_time,
                response.status_code if hasattr(response, 'status_code') else 500,
                None if success else str(response)
            )

            if not success:
                return False

            json_data = response.json()
            normalized_data = self.normalize_json_data(json_data)
            row_count = self.data_processor.split_daily_data(country, normalized_data, target_date, target_date)
            return row_count > 0

        except Exception as e:
            self.logger.error(f"Error extracting COVID-19 data for {country}: {str(e)}")
            return False


    def extract_single_day_data(self, countries, specific_date=None, start_date=None, end_date=None):
        success_count = 0
        total_count = len(countries)

        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            futures = {}

            for country in countries:
                if specific_date:
                    futures[executor.submit(self.extract_single_day_for_country, country, specific_date)] = country
                elif start_date and end_date:
                    for date in self._date_range(start_date, end_date):
                        futures[executor.submit(self.extract_single_day_for_country, country, date)] = f"{country}_{date.strftime('%Y-%m-%d')}"
                else:
                    futures[executor.submit(self.extract_single_day_for_country, country)] = country

            for future in as_completed(futures):
                label = futures[future]
                try:
                    result = future.result()
                    if result:
                        success_count += 1
                        self.logger.info(f"COVID-19 data extraction for {label} completed successfully")
                    else:
                        self.logger.error(f"COVID-19 data extraction for {label} failed")
                except Exception as e:
                    self.logger.error(f"Error processing COVID-19 data for {label}: {str(e)}")

        self.logger.info(f"Single day COVID-19 data extraction completed: {success_count}/{total_count} successful")
        return success_count == total_count
