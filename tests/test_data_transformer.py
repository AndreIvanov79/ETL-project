import pytest
from unittest.mock import MagicMock, call
from datetime import datetime
from src.transform.data_transformer import CommonDataTransformer


@pytest.fixture
def mock_dependencies(mocker):
    mocker.patch('src.transform.data_transformer.SchemaValidator')
    mocker.patch('src.transform.data_transformer.RequiredRule')
    mocker.patch('src.transform.data_transformer.DateFormatRule')
    mocker.patch('src.transform.data_transformer.NumericRangeRule')
    
    mock_db = mocker.patch('src.transform.data_transformer.DBManager')
    mock_db_instance = MagicMock()
    mock_db_instance.get_connection.return_value = True
    mock_db.return_value = mock_db_instance

    mock_covid = mocker.patch('src.transform.data_transformer.CovidTransformer')
    mock_weather = mocker.patch('src.transform.data_transformer.WeatherTransformer')
    mock_error = mocker.patch('src.transform.data_transformer.ErrorManager')

    return {
        "db": mock_db_instance,
        "covid": mock_covid.return_value,
        "weather": mock_weather.return_value,
        "error": mock_error.return_value,
        "db_class": mock_db
    }


def test_transform_all_success(mock_dependencies):
    mock_dependencies["covid"].transform.return_value = 10
    mock_dependencies["weather"].transform.return_value = 20

    transformer = CommonDataTransformer()
    results = transformer.transform_all(countries=["greece"])

    assert results["covid"] == 10
    assert results["weather"] == 20
    transformer.close()


def test_transform_covid_raises_exception(mock_dependencies):
    mock_dependencies["covid"].transform.side_effect = Exception("COVID transform error")
    mock_dependencies["weather"].transform.return_value = 15

    transformer = CommonDataTransformer()
    results = transformer.transform_all(countries=["thailand"])

    assert results["covid"] == 0
    assert results["weather"] == 15
    transformer.close()


def test_transform_weather_raises_exception(mock_dependencies):
    mock_dependencies["covid"].transform.return_value = 7
    mock_dependencies["weather"].transform.side_effect = Exception("Weather transform error")

    transformer = CommonDataTransformer()
    results = transformer.transform_all(countries=["norway"])

    assert results["covid"] == 7
    assert results["weather"] == 0
    transformer.close()


def test_transform_all_multiple_countries(mock_dependencies):
    mock_dependencies["covid"].transform.side_effect = [1, 2, 3]
    mock_dependencies["weather"].transform.side_effect = [5, 10, 15]

    transformer = CommonDataTransformer()
    results = transformer.transform_all(countries=["greece", "thailand", "norway"])

    assert results["covid"] == 6
    assert results["weather"] == 30
    transformer.close()


def test_close_calls_db_close_and_logs_message(mock_dependencies, mock_logger):
    transformer = CommonDataTransformer()
    transformer.close()

    mock_dependencies["db"].close.assert_called_once()
    mock_logger.info.assert_called_with("Database connection closed.")


def test_transform_logs_for_each_country(mock_dependencies, mock_logger):
    mock_dependencies["covid"].transform.return_value = 1
    mock_dependencies["weather"].transform.return_value = 1

    transformer = CommonDataTransformer()
    transformer.transform_all(countries=["greece"])

    mock_logger.info.assert_has_calls([
        call("Starting COVID transform for greece"),
        call("Starting WEATHER transform for greece"),
        call("Total COVID records processed: 1"),
        call("Total WEATHER records processed: 1")
    ], any_order=False)

    transformer.close()
