import unittest
import pytest
from unittest.mock import MagicMock
from src.extract.data_extraction import DataExtraction
from src.error_handling.error_handling import ErrorCode, ErrorSeverity


@pytest.fixture
def mock_dependencies(mocker):
    mocker.patch('src.extract.data_extraction.DirectoryManager')
    mocker.patch('src.extract.data_extraction.ApiClient')
    mocker.patch('src.extract.data_extraction.WeatherDataProcessor')
    mocker.patch('src.extract.data_extraction.CovidDataProcessor')

    mock_db = mocker.patch('src.extract.data_extraction.DBManager')
    mock_db_instance = MagicMock()
    mock_db_instance.get_connection.return_value = True
    mock_db.return_value = mock_db_instance

    mock_weather = mocker.patch('src.extract.data_extraction.WeatherExtractor')
    mock_covid = mocker.patch('src.extract.data_extraction.CovidExtractor')
    mock_error = mocker.patch('src.extract.data_extraction.ErrorManager')

    return {
        "db": mock_db_instance,
        "weather": mock_weather.return_value,
        "covid": mock_covid.return_value,
        "error": mock_error.return_value
    }


def test_extract_all_data_success(mock_dependencies):
    mock_dependencies["weather"].extract_single_day_data.return_value = True
    mock_dependencies["covid"].extract_single_day_data.return_value = True

    extractor = DataExtraction(countries=["greece", "mexico"])
    result = extractor.extract_all_data()

    assert result is True


def test_extract_all_data_partial_failure(mock_dependencies):
    mock_dependencies["weather"].extract_single_day_data.return_value = True
    mock_dependencies["covid"].extract_single_day_data.return_value = False

    extractor = DataExtraction(countries=["mexico"])
    result = extractor.extract_all_data()

    assert result is False


def test_extract_all_data_raises_exception(mock_dependencies):
    mock_dependencies["weather"].extract_single_day_data.side_effect = Exception("Test crash")

    extractor = DataExtraction(countries=["thailand"])
    result = extractor.extract_all_data()

    assert result is False
    mock_dependencies["error"].create_error.assert_called_with(
        code=ErrorCode.UNKNOWN_ERROR,
        message=unittest.mock.ANY,
        severity=ErrorSeverity.CRITICAL,
        component="DataExtraction.extract_all_data",
        details=unittest.mock.ANY
    )

def test_close_logs_and_closes_connection(mock_dependencies, mock_logger):
    transformer = DataExtraction()
    transformer.close()

    mock_dependencies["db"].close.assert_called_once()
    mock_logger.info.assert_not_called()  
