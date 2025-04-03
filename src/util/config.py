import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

class Config:

    RAPIDAPI_KEY = os.getenv('RAPIDAPI_KEY', '')
    
    today = datetime.now()
    START_DATE = datetime(2022, today.month, today.day)
    END_DATE = datetime(2022, today.month, today.day)
    
    DB_PATH = '../../etl_data.duckdb'
    
    DATA_DIR = 'data'
    
    MAX_RETRIES = 3
    RETRY_DELAY = 5  
    
    MAX_WORKERS = 4
    
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
