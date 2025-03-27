import os
from dotenv import load_dotenv
from src.extract.data_extraction import DataExtractor

def main():
    load_dotenv()

    extractor = DataExtractor()

    extractor.extract_weather_data()
    extractor.extract_covid_data()

if __name__ == '__main__':
    main()
