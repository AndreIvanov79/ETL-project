import pytest
import os
import json
import csv
from unittest.mock import patch, MagicMock, mock_open
from src.load import data_loader


def test_save_csv(tmp_path):
    file_path = tmp_path / "test.csv"
    headers = ["id", "name"]
    rows = [[1, "Alice"], [2, "Bob"]]

    data_loader.save_csv(file_path, headers, rows)

    with open(file_path, newline='', encoding='utf-8') as f:
        reader = list(csv.reader(f))
        assert reader == [["id", "name"], ["1", "Alice"], ["2", "Bob"]]


def test_save_json(tmp_path):
    file_path = tmp_path / "test.json"
    headers = ["id", "date"]
    rows = [[1, "2020-01-01T12:00:00"]]

    data_loader.save_json(file_path, headers, rows)

    with open(file_path, encoding='utf-8') as f:
        result = json.load(f)
        assert result[0]["id"] == 1
        assert "T" in result[0]["date"]


def test_get_column_names_success(mocker):
    mock_cursor = MagicMock()
    mock_cursor.description = [("id",), ("name",)]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.execute.return_value = None

    mocker.patch.object(data_loader.db, "get_connection", return_value=mock_conn)

    result = data_loader.get_column_names("reporting_weather_data")
    assert result == ["id", "name"]


def test_get_column_names_fallback(mocker):
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = Exception("DB error")
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mocker.patch.object(data_loader.db, "get_connection", return_value=mock_conn)

    result = data_loader.get_column_names("reporting_covid_19_data")
    assert result == ["country_id", "date", "cases"]


def test_process_table_data_parses_datetime():
    raw = [["2024-01-01T10:00:00", "ok"], ["just text", "value"]]
    processed = data_loader.process_table_data("any_table", raw)
    assert isinstance(processed[0][0], object)  # datetime or str
    assert processed[1][0] == "just text"


def test_save_table_no_rows(capfd):
    data_loader.save_table("any_table", [])
    captured = capfd.readouterr()
    assert "Нет данных в таблице" in captured.out


def test_save_table_no_headers(mocker, capfd):
    mocker.patch("src.load.data_loader.get_column_names", return_value=[])
    data_loader.save_table("unknown_table", [["x"]])
    captured = capfd.readouterr()
    assert "Не удалось определить структуру таблицы" in captured.out


def test_prepare_reporting_tables_success(mocker, capfd):
    mocker.patch.object(data_loader.db, "prepare_reporting_tables", return_value=True)
    data_loader.prepare_reporting_tables()
    captured = capfd.readouterr()
    assert "Таблицы отчетности обновлены успешно" in captured.out


def test_prepare_reporting_tables_failure(mocker, capfd):
    mocker.patch.object(data_loader.db, "prepare_reporting_tables", side_effect=Exception("Fail"))
    data_loader.prepare_reporting_tables()
    captured = capfd.readouterr()
    assert "Непредвиденная ошибка при подготовке таблиц отчетности" in captured.out
