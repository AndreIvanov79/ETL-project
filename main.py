import os
from dotenv import load_dotenv
from datetime import datetime

def main():
    print(f"Starting ETL process at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("===========================================================")
    
    # Import after the banner to avoid printing before the banner
    from extract.data_extraction import DataExtractor
    from src.transform.data_transformer import DataTransformer
    from src.load.data_loading import DataLoader
    from src.logging.logger import setup_logger
    
    # Load environment variables
    load_dotenv()

    # Setup logger
    logger = setup_logger()

    # Create components
    extractor = DataExtractor()
    transformer = DataTransformer(logger)
    loader = DataLoader(db_path='etl_data.duckdb', logger=logger)
    
    countries = ['greece', 'thailand', 'norway']

    # Extract data
    print("Extracting weather data...")
    extractor.extract_weather_data()

    print("Extracting COVID-19 data...")
    extractor.extract_covid_data()
    
    # Transform data
    print("Transforming weather data...")
    weather_df = transformer.transform_weather_data(countries)
    
    print("Transforming COVID-19 data...")
    covid_df = transformer.transform_covid_data(countries)
    
    # Load data
    print("Loading data to database...")
    if loader.connect():
        loader.load_weather_data(weather_df)
        loader.load_covid_data(covid_df)
        loader.close()
    
    print("===========================================================")
    print(f"ETL process completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == '__main__':
    main()




# import os
# from dotenv import load_dotenv
# from datetime import datetime

# def main():
#     print(f"Starting ETL process at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
#     print("===========================================================")
    
#     # Import after the banner to avoid printing before the banner
#     from src.extract.data_extraction import DataExtractor
    
#     # Load environment variables
#     load_dotenv()

#     # Create extractor
#     extractor = DataExtractor()

#     # Extract weather data
#     print("Extracting weather data...")
#     extractor.extract_weather_data()

#     # Extract COVID-19 data
#     print("Extracting COVID-19 data...")
#     extractor.extract_covid_data()
    
#     print("===========================================================")
#     print(f"ETL process completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# if __name__ == '__main__':
#     main()
    