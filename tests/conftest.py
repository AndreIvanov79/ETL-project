import pytest
from unittest.mock import MagicMock

@pytest.fixture(autouse=True)
def mock_logger(mocker):
    mock = MagicMock()
    mocker.patch("src.transform.data_transformer.setup_logger", return_value=mock)
    mocker.patch("src.extract.data_extraction.setup_logger", return_value=mock)
    return mock
