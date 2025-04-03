from datetime import datetime, timedelta
import json
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
            
            url = "https://meteostat.p.rapidapi.com/point/daily"
            
            querystring = {
                "lat": coords['lat'],
                "lon": coords['lon'],
                "alt": coords['alt'],
                "start": Config.START_DATE.strftime("%Y-%m-%d"),
                "end": Config.END_DATE.strftime("%Y-%m-%d")
            }

            headers = {
                "x-rapidapi-key": Config.RAPIDAPI_KEY,
                "x-rapidapi-host": "meteostat.p.rapidapi.com"
            }

            self.logger.info(f'Extracting weather data for {country} from {Config.START_DATE} to {Config.END_DATE}')
            response, success = self.api_client.make_request(
                "meteostat",
                country,
                url,
                headers=headers,
                params=querystring
            )
            
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
                        country,
                        dir_name,
                        file_name,
                        row_count
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

            target_date = specific_date or datetime(2022, datetime.now().month, datetime.now().day)
            date_str = target_date.strftime("%Y-%m-%d")

            url = "https://meteostat.p.rapidapi.com/point/daily"
            querystring = {
                "lat": coords['lat'],
                "lon": coords['lon'],
                "alt": coords['alt'],
                "start": date_str,
                "end": date_str
            }

            headers = {
                "x-rapidapi-key": Config.RAPIDAPI_KEY,
                "x-rapidapi-host": "meteostat.p.rapidapi.com"
            }

            self.logger.info(f'Extracting weather data for {country} for {date_str}')
            response, success = self.api_client.make_request("meteostat", country, url, headers=headers, params=querystring)

            self.db.log_api_call(
                country,
                "meteostat",
                datetime.now(),
                datetime.now(),
                response.status_code if hasattr(response, 'status_code') else 500,
                None if success else str(response)
            )

            if success:
                json_data = response.json()

                row_count = self.data_processor.split_daily_data(
                    country,
                    json_data,
                    target_date,
                    target_date
                )

                return row_count > 0

            return False

        except Exception as e:
            self.logger.error(f'Error extracting weather data for {country}: {str(e)}')
            return False
        
    def extract_single_day_data(self, countries):
        success_count = 0
        total_count = len(countries)
        
        for country in countries:
            try:
                result = self.extract_single_day_for_country(country)
                if result:
                    success_count += 1
                    self.logger.info(f"Single day weather data extraction for {country} completed successfully")
                else:
                    self.logger.error(f"Single day weather data extraction for {country} failed")
            except Exception as e:
                self.logger.error(f"Error processing single day weather data for {country}: {str(e)}")
        
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
                self.logger.error(f'No country code found for {country}')
                return False
            
            url = f"https://disease.sh/v3/covid-19/historical/{country}?lastdays=all"
            
            self.logger.info(f'Extracting COVID-19 data for {country}')
            response, success = self.api_client.make_request(
                "disease.sh",
                country, 
                url
            )
            
            if success:
                json_data, dir_name, file_name = self.data_processor.save_response(
                    country,
                    'covid',
                    response,
                    'covid_data_complete.json'
                )
                
                if json_data:
                    row_count = self.data_processor.split_daily_data(
                        country, 
                        json_data,
                        Config.START_DATE, 
                        Config.END_DATE
                    )
                    
                    self.db.log_file_import(
                        country,
                        dir_name,
                        file_name,
                        row_count
                    )
                    
                    return True
            
            return False
            
        except Exception as e:
            self.logger.error(f'Unexpected error extracting COVID-19 data for {country}: {str(e)}')
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
                        self.logger.info(f"COVID-19 data extraction for {country} completed successfully")
                    else:
                        self.logger.error(f"COVID-19 data extraction for {country} failed")
                except Exception as e:
                    self.logger.error(f"Error processing COVID-19 data for {country}: {str(e)}")
        
        self.logger.info(f"COVID-19 data extraction completed: {success_count}/{total_count} successful")
        return success_count == total_count
        
    def extract_single_day_for_country(self, country, specific_date=None):
        try:
            country_code = Config.COUNTRY_CODES.get(country.lower())
            if not country_code:
                self.logger.error(f'No country code found for {country}')
                return False

            current_date = specific_date or datetime.now()
            target_date = datetime(2022, current_date.month, current_date.day)
            target_date_str = target_date.strftime('%Y-%m-%d')

            url = f"https://disease.sh/v3/covid-19/historical/{country}?lastdays=all"

            self.logger.info(f'Extracting COVID-19 data for {country} for {target_date_str}')
            start_time = datetime.now()
            response, success = self.api_client.make_request("disease.sh", country, url)
            end_time = datetime.now()

            self.db.log_api_call(
                country,
                "disease.sh",
                start_time,
                end_time,
                response.status_code if hasattr(response, 'status_code') else 500,
                None if success else str(response)
            )

            if not success or not response:
                self.logger.error(f'API call failed for {country}')
                return False

            json_data = response.json()

            os.makedirs("logs", exist_ok=True)
            with open(f"logs/covid_raw_{country}.json", "w") as f:
                json.dump(json_data, f, indent=2)

            timeline = json_data.get('timeline') if 'timeline' in json_data else json_data
            if not timeline or 'cases' not in timeline or not timeline['cases']:
                self.logger.error(f'No timeline data or no "cases" for {country}')
                return False

            row_count = self.data_processor.split_daily_data(
                country,
                json_data,
                target_date,
                target_date
            )

            return row_count > 0

        except Exception as e:
            self.logger.error(f'Error extracting COVID data for {country}: {str(e)}')
            return False

    def extract_single_day_data(self, countries, specific_date=None, start_date=None, end_date=None):
            success_count = 0
            total_count = len(countries)

            for country in countries:
                try:
                    if specific_date:
                        result = self.extract_single_day_for_country(country, specific_date)
                    elif start_date and end_date:
                        result = all(
                            self.extract_single_day_for_country(country, current_date)
                            for current_date in self._date_range(start_date, end_date)
                        )
                    else:
                        result = self.extract_single_day_for_country(country)  

                    if result:
                        success_count += 1
                        self.logger.info(f"COVID-19 data extraction for {country} completed successfully")
                    else:
                        self.logger.error(f"COVID-19 data extraction for {country} failed")

                except Exception as e:
                    self.logger.error(f"Error processing COVID-19 data for {country}: {str(e)}")

            self.logger.info(f"COVID-19 extraction finished: {success_count}/{total_count} successful")
            return success_count == total_count
