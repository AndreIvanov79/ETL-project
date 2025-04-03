import json
import os
from src.logging.logger import setup_logger
from datetime import datetime
from src.util.config import Config

class DataProcessor:
    def __init__(self, db):
        self.logger = setup_logger()
        self.db = db
    
    def save_response(self, country, data_type, response, filename):
        base_dir = f'{Config.DATA_DIR}/{data_type}/{country}'
        filepath = f'{base_dir}/{filename}'
        
        try:
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            
            with open(filepath, 'w') as f:
                json.dump(response.json(), f, indent=4)
            
            self.logger.info(f'Successfully saved {data_type} data for {country} to {filepath}')
            return response.json(), os.path.dirname(filepath), os.path.basename(filepath)
        
        except Exception as e:
            self.logger.error(f'Error saving {data_type} data for {country}: {str(e)}')
            return None, None, None

class WeatherDataProcessor(DataProcessor):
    
    def split_daily_data(self, country, json_data, start_date=None, end_date=None):
        self.logger.info(f'Splitting weather data for {country} into daily files')
        row_count = 0

        start_date = start_date or Config.START_DATE
        end_date = end_date or Config.END_DATE

        try:
            if 'data' in json_data:
                for day_data in json_data['data']:
                    if 'date' not in day_data:
                        self.logger.warning(f"Skipping weather entry without 'date': {day_data}")
                        continue

                    date_str = day_data['date']

                    try:
                        try:
                            date_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                        except ValueError:
                            date_obj = datetime.strptime(date_str, '%Y-%m-%d')

                        if not (start_date <= date_obj <= end_date):
                            self.logger.debug(f"Weather data for {date_obj.date()} is outside requested range.")
                            continue

                        month_dir = os.path.join(f'{Config.DATA_DIR}/weather/{country}', date_obj.strftime('%Y-%m'))
                        os.makedirs(month_dir, exist_ok=True)

                        file_path = os.path.join(month_dir, f"{date_obj.strftime('%d')}.json")
                        with open(file_path, 'w') as f:
                            json.dump(day_data, f, indent=4)

                        self.logger.info(f'Saved weather data for {date_obj.strftime("%Y-%m-%d")}')
                        self.db.log_file_import(country, month_dir, f"{date_obj.strftime('%d')}.json", 1)
                        row_count += 1

                    except ValueError:
                        self.logger.error(f'Invalid date format in weather data: {date_str}')
            else:
                self.logger.error(f'No "data" field found in weather JSON for {country}')

        except Exception as e:
            self.logger.error(f'Error splitting weather data for {country}: {str(e)}')

        return row_count

class CovidDataProcessor(DataProcessor):
    
    def split_daily_data(self, country, json_data, start_date=None, end_date=None):
        self.logger.info(f'Splitting COVID data for {country} into daily files')
        row_count = 0

        start_date = start_date or Config.START_DATE
        end_date = end_date or Config.END_DATE

        try:
            timeline = json_data.get('timeline') if 'timeline' in json_data else json_data

            if timeline and 'cases' in timeline:
                for date_str, cases in timeline['cases'].items():
                    try:
                        date_obj = datetime.strptime(date_str, '%m/%d/%y')
                        self.logger.debug(f"Checking COVID date: {date_str} -> {date_obj.strftime('%Y-%m-%d')}")

                        if not (start_date <= date_obj <= end_date):
                            self.logger.debug(f"Skipped {date_obj.strftime('%Y-%m-%d')} — out of range")
                            continue

                        daily_data = {
                            'date': date_obj.strftime('%Y-%m-%d'),
                            'cases': cases,
                            'deaths': timeline['deaths'].get(date_str, 0),
                            'recovered': timeline['recovered'].get(date_str, 0) if 'recovered' in timeline else None
                        }

                        month_dir = os.path.join(f'{Config.DATA_DIR}/covid/{country}', date_obj.strftime('%Y-%m'))
                        os.makedirs(month_dir, exist_ok=True)

                        file_path = os.path.join(month_dir, f"{date_obj.strftime('%d')}.json")
                        with open(file_path, 'w') as f:
                            json.dump(daily_data, f, indent=4)

                        self.logger.info(f'Saved COVID data for {date_obj.strftime("%Y-%m-%d")}')
                        self.db.log_file_import(country, month_dir, f"{date_obj.strftime('%d')}.json", 1)
                        row_count += 1

                    except ValueError:
                        self.logger.error(f'Invalid COVID date format: {date_str}')
            else:
                self.logger.error(f'Missing timeline or cases in COVID data for {country}')

        except Exception as e:
            self.logger.error(f'Error splitting COVID data for {country}: {str(e)}')

        return row_count
