import requests
import time
from src.logging.logger import setup_logger
from datetime import datetime
from src.util.config import Config

class ApiClient:
    def __init__(self, db):
        self.logger = setup_logger()
        self.db = db
        
    def make_request(self, api_id, country, url, headers=None, params=None, max_retries=None):
    
        if max_retries is None:
            max_retries = Config.MAX_RETRIES
            
        retry_count = 0
        success = False
        response = None
        
        while retry_count <= max_retries and not success:
            try:
                start_time = datetime.now()
                
                if retry_count > 0:
                    self.logger.info(f"Retry attempt {retry_count} for {api_id} API call for {country}")
                    
                response = requests.get(url, headers=headers, params=params, timeout=30)
                
                end_time = datetime.now()
                
                if response.status_code == 200:
                    success = True
                    error_message = None
                else:
                    error_message = response.text
                    self.logger.error(f"API call failed with status code {response.status_code}: {error_message}")
                
                self.db.log_api_call(
                    country,
                    api_id,
                    start_time,
                    end_time,
                    response.status_code,
                    error_message
                )
                
                if not success:
                    retry_count += 1
                    if retry_count <= max_retries:
                        time.sleep(Config.RETRY_DELAY)
                    
            except requests.RequestException as e:
                end_time = datetime.now()
                error_message = str(e)
                self.logger.error(f"Request error: {error_message}")
                
                self.db.log_api_call(
                    country,
                    api_id,
                    start_time,
                    end_time,
                    0,  
                    error_message
                )
                
                retry_count += 1
                if retry_count <= max_retries:
                    time.sleep(Config.RETRY_DELAY)
        
        return response, success
