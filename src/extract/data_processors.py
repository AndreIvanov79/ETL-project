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
        base_dir = f'{Config.DATA_DIR}/{data_type}/2020-2021/{country}'
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
        self.logger.info(f'Splitting daily weather data for {country}')
        row_count = 0

        start_date = start_date or Config.START_DATE
        end_date = end_date or Config.END_DATE

        try:
            daily = json_data.get("daily", {})
            if not daily or "time" not in daily:
                self.logger.error(f"No 'daily' or 'time' data found in weather JSON for {country}")
                return 0

            time_list = daily["time"]
            for i, date_str in enumerate(time_list):
                try:
                    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                    if not (start_date <= date_obj <= end_date):
                        continue

                    day_data = {k: v[i] for k, v in daily.items() if k != "time"}

                    output_data = {
                        "country": country,
                        "date": date_str,
                        "data": day_data
                    }

                    month_dir = os.path.join(
                        f"{Config.DATA_DIR}/weather/2020-2021/{country}",
                        date_obj.strftime("%Y-%m")
                    )
                    os.makedirs(month_dir, exist_ok=True)

                    file_path = os.path.join(month_dir, f"{date_obj.strftime('%d')}.json")
                    with open(file_path, 'w') as f:
                        json.dump(output_data, f, indent=4)

                    self.logger.info(f'Saved weather data for {date_str}')
                    self.db.log_file_import(country, month_dir, f"{date_obj.strftime('%d')}.json", 1)
                    row_count += 1

                except Exception as e:
                    self.logger.error(f"Error processing weather date {date_str}: {str(e)}")

        except Exception as e:
            self.logger.error(f"Failed to split weather data for {country}: {str(e)}")

        return row_count


class CovidDataProcessor(DataProcessor):

    def split_daily_data(self, country, json_data, start_date=None, end_date=None):
        self.logger.info(f'Splitting COVID data for {country} into daily files')
        row_count = 0

        start_date = start_date or Config.START_DATE
        end_date = end_date or Config.END_DATE

        try:
            for row in json_data:
                if 'date' not in row:
                    self.logger.warning(f"Skipping row without date: {row}")
                    continue

                try:
                    date_obj = datetime.strptime(row['date'], '%Y-%m-%d')

                    if not (start_date <= date_obj <= end_date):
                        continue

                    month_dir = os.path.join(
                        f'{Config.DATA_DIR}/covid/2020-2021/{country}',
                        date_obj.strftime('%Y-%m')
                    )
                    os.makedirs(month_dir, exist_ok=True)

                    file_path = os.path.join(month_dir, f"{date_obj.strftime('%d')}.json")
                    with open(file_path, 'w') as f:
                        json.dump(row, f, indent=4)

                    self.logger.info(f'Saved COVID data for {date_obj.strftime("%Y-%m-%d")}')
                    self.db.log_file_import(country, month_dir, f"{date_obj.strftime('%d')}.json", 1)
                    row_count += 1

                except ValueError as ve:
                    self.logger.error(f"Invalid date format: {row['date']} — {ve}")

        except Exception as e:
            self.logger.error(f'Error splitting COVID data for {country}: {str(e)}')

        return row_count
