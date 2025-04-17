import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

class Config:

    RAPIDAPI_KEY = os.getenv('RAPIDAPI_KEY', '')

    API_KEY = os.getenv('API_KEY', '')

    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    today = datetime.now()
    START_DATE = datetime(2022, today.month, today.day)
    END_DATE = datetime(2022, today.month, today.day) 

    TOTAL_START_DATE = datetime.strptime('2020-01-01', "%Y-%m-%d")
    TOTAL_END_DATE = datetime.strptime('2021-01-01', "%Y-%m-%d")
    
    DB_PATH = os.getenv('DB_PATH', os.path.join(PROJECT_ROOT, 'etl_data.duckdb'))
    
    DATA_DIR = 'src/extract/data'
    
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
