import json
import os
from datetime import datetime
from collections import defaultdict
from src.logging.logger import setup_logger
from src.util.config import Config

class DataProcessor:
    
    def __init__(self, db):
        self.logger = setup_logger()
        self.db = db

    def save_response(self, country, data_type, response, filename=None):
        try:
            data = response.json()
        except ValueError as e:
            self.logger.error(f"Failed to parse JSON response for {country}: {e}")
            return []

        periods = set()
        if data_type == 'weather' and isinstance(data, dict):
            periods = {d[:7] for d in data.get('daily', {}).get('time', []) if isinstance(d, str)}
        elif data_type == 'covid' and isinstance(data, list):
            periods = {row.get('date','')[:7] for row in data if 'date' in row}
        if not periods:
            periods = { datetime.now().strftime('%Y-%m') }

        saved = []
        for period in periods:
            base = os.path.join(Config.DATA_DIR, data_type, country, period)
            os.makedirs(base, exist_ok=True)
            fname = filename or f"{data_type}_{country}_{period}.json"
            path = os.path.join(base, fname)
            try:
                with open(path, 'w') as f:
                    json.dump(data, f, indent=4)
                self.logger.info(f"Saved raw {data_type} for {country} → {path}")
                self.db.log_file_import(country, base, fname, 1)
                saved.append(path)
            except Exception as e:
                self.logger.error(f"Error saving {data_type} for {country}: {e}")
        return saved

class WeatherDataProcessor(DataProcessor):
    def split_daily_data(self, country, json_data, start_date=None, end_date=None):
        self.logger.info(f"Splitting daily weather for {country}")
        records_by_month = defaultdict(list)
        daily = json_data.get('daily', {})
        times = daily.get('time', [])
        for i, ds in enumerate(times):
            try:
                dt = datetime.strptime(ds, '%Y-%m-%d')
            except ValueError:
                self.logger.error(f"Bad weather date: {ds}")
                continue
            m = ds[:7]
            rec = {
                'country_id': country,
                'date': ds,
                'tavg': daily.get('temperature_2m_mean',[None])[i],
                'tmin': daily.get('temperature_2m_min',[None])[i],
                'tmax': daily.get('temperature_2m_max',[None])[i],
                'prcp': daily.get('precipitation_sum',[None])[i],
                'snow': None,
                'wdir': None,
                'wspd': daily.get('wind_speed_10m_max',[None])[i],
                'wpgt': None,
                'pres': None,
                'tsun': daily.get('shortwave_radiation_sum',[None])[i]
            }
            records_by_month[m].append(rec)
        total = 0
        for m, recs in records_by_month.items():
            d = os.path.join(Config.DATA_DIR, 'weather', country, m)
            os.makedirs(d, exist_ok=True)
            out = os.path.join(d, f"{m}.json")
            try:
                with open(out, 'w') as f:
                    json.dump(recs, f, indent=4)
                self.logger.info(f"Saved weather {country}/{m}: {len(recs)} records")
                self.db.log_file_import(country, d, os.path.basename(out), len(recs))
                total += len(recs)
            except Exception as e:
                self.logger.error(f"Error writing weather for {country}/{m}: {e}")
        return total

class CovidDataProcessor(DataProcessor):
    def split_daily_data(self, country, json_data, start_date=None, end_date=None):
        self.logger.info(f"Splitting daily COVID for {country}")
        start = start_date or Config.START_DATE
        end = end_date or Config.END_DATE
        count = 0
        for row in json_data:
            ds = row.get('date')
            if not ds:
                self.logger.warning(f"Skipping COVID row without date: {row}")
                continue
            try:
                dt = datetime.strptime(ds, '%Y-%m-%d')
            except ValueError:
                self.logger.error(f"Bad COVID date: {ds}")
                continue
            if not (start <= dt <= end):
                continue
            m = ds[:7]
            d = os.path.join(Config.DATA_DIR, 'covid', country, m)
            os.makedirs(d, exist_ok=True)
            out = os.path.join(d, f"{ds[8:10]}.json")
            try:
                with open(out, 'w') as f:
                    json.dump(row, f, indent=4)
                self.db.log_file_import(country, d, os.path.basename(out), 1)
                count += 1
            except Exception as e:
                self.logger.error(f"Error saving COVID for {country}/{ds}: {e}")
        return count
