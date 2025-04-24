from datetime import datetime, timedelta
import json
import requests
from src.logging.logger import setup_logger
import os
from src.util.config import Config
from enum import Enum

class ApiType(Enum):
    COVID_URL = "covid"
    WEATHER_URL = "weather"

class GenericExtractor:
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

    def normalize_json_data(self, json_data):
        
        if isinstance(json_data, dict) and "columns" in json_data:
            cols = json_data.get('columns', [])
            rows = json_data.get('data', [])
            return [ { cols[i]: val for i, val in enumerate(row) } for row in rows ]
        return json_data

    def extract_for_country(self, country, api_type, specific_date=None, start_date=None, end_date=None):
        
        country_id = self.db.get_country_id(country)
        if country_id is None:
            self.logger.error(f'Country {country} not found in database')
            return False

        if api_type == ApiType.COVID_URL:
            code = Config.COUNTRY_CODES.get(country.lower())
            if not code:
                self.logger.error(f'No country code for {country}')
                return False
            identifier, api_name, log_name = code, 'covid-19', 'covid'
        else:
            coords = Config.COUNTRY_COORDINATES.get(country.lower())
            if not coords:
                self.logger.error(f'No coordinates for {country}')
                return False
            identifier, api_name, log_name = coords, 'meteostat', 'meteostat'

        if specific_date:
            start_date = end_date = specific_date
            if api_type == ApiType.COVID_URL:
                start_date = end_date = datetime(2022, specific_date.month, specific_date.day)
        elif not (start_date and end_date):
            start_date, end_date = Config.START_DATE, Config.END_DATE

        start_str, end_str = start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
        
        if api_type == ApiType.COVID_URL:
            url = f"https://storage.googleapis.com/covid19-open-data/v3/location/{identifier}.json"
            headers = {}
        else:
            url = (
                f"https://archive-api.open-meteo.com/v1/archive?latitude={identifier['lat']}"
                f"&longitude={identifier['lon']}&start_date={start_str}&end_date={end_str}"  
                "&daily=temperature_2m_mean,temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max,shortwave_radiation_sum"
            )
            headers = {}

        self.logger.info(f'Extracting {api_type.value} for {country} from {start_str} to {end_str}')
        t0 = datetime.now()
        response, success = self.api_client.make_request(api_name, country, url, headers=headers)
        t1 = datetime.now()

        self.db.log_api_call(
            country_id,
            log_name,
            t0, t1,
            getattr(response, 'status_code', 500),
            None if success else str(response)
        )

        if not success:
            return False

        data = response.json()
        data = self.normalize_json_data(data)

        if specific_date or api_type == ApiType.COVID_URL or start_date == end_date:
            count = self.data_processor.split_daily_data(country, data, start_date, end_date)
            return count > 0
        
        saved = self.data_processor.save_response(country, api_type.value, response,
                                                  f'{api_type.value}_data_complete_{country}.json')
        if not saved:
            return False
        
        count = self.data_processor.split_daily_data(country, data, start_date, end_date)
        return True

    def extract_data(self, countries, api_type, specific_date=None, start_date=None, end_date=None):
        success = 0
        tasks = []
        
        if specific_date or (start_date and end_date and start_date == end_date):
            date = specific_date or start_date
            for c in countries:
                tasks.append((c, date))
        elif start_date and end_date and start_date != end_date:
            for c in countries:
                for d in self._date_range(start_date, end_date):
                    tasks.append((c, d))
        else:
            for c in countries:
                tasks.append((c, None))

        total = len(tasks)
        for country, date in tasks:
            label = country if date is None else f"{country}_{date.strftime('%Y-%m-%d')}"
            try:
                ok = self.extract_for_country(country, api_type, specific_date=date,
                                              start_date=start_date, end_date=end_date)
                if ok:
                    success += 1
                    self.logger.info(f"{api_type.value} extraction for {label} succeeded")
                else:
                    self.logger.error(f"{api_type.value} extraction for {label} failed")
            except Exception as e:
                self.logger.error(f"Error in extraction for {label}: {e}")

        etype = 'single day' if (specific_date or (start_date and end_date and start_date == end_date)) else 'data range'
        self.logger.info(f"{api_type.value} {etype} extraction: {success}/{total} successful")
        return success == total

class WeatherExtractor:
    def __init__(self, api_client, data_processor, db):
        self.extractor = GenericExtractor(api_client, data_processor, db)
    def extract_for_country(self, country, specific_date=None, start_date=None, end_date=None):
        return self.extractor.extract_for_country(country, ApiType.WEATHER_URL, specific_date, start_date, end_date)
    def extract_data(self, countries, specific_date=None, start_date=None, end_date=None):
        return self.extractor.extract_data(countries, ApiType.WEATHER_URL, specific_date, start_date, end_date)
    def extract_single_day_for_country(self, country, specific_date=None):
        return self.extractor.extract_for_country(country, ApiType.WEATHER_URL, specific_date)
    def extract_single_day_data(self, countries, specific_date=None, start_date=None, end_date=None):
        return self.extractor.extract_data(countries, ApiType.WEATHER_URL, specific_date, start_date, end_date)

class CovidExtractor:
    def __init__(self, api_client, data_processor, db):
        self.extractor = GenericExtractor(api_client, data_processor, db)
    def extract_for_country(self, country, specific_date=None, start_date=None, end_date=None):
        return self.extractor.extract_for_country(country, ApiType.COVID_URL, specific_date, start_date, end_date)
    def extract_data(self, countries, specific_date=None, start_date=None, end_date=None):
        return self.extractor.extract_data(countries, ApiType.COVID_URL, specific_date, start_date, end_date)
    def extract_single_day_for_country(self, country, specific_date=None):
        return self.extractor.extract_for_country(country, ApiType.COVID_URL, specific_date)
    def extract_single_day_data(self, countries, specific_date=None, start_date=None, end_date=None):
        return self.extractor.extract_data(countries, ApiType.COVID_URL, specific_date, start_date, end_date)
