import json
from unittest.mock import patch

import pytest

from pattern_impl import CsvToJsonPipeline, JsonStatsPipeline

# ---------------------------------------------------------------------------
# Позитив — общий алгоритм и overriding
# ---------------------------------------------------------------------------

def test_csv_to_json_pipeline():
    raw_csv = "name,age\nAnn,30\nBob,25\n"
    pipeline = CsvToJsonPipeline(reader=lambda p: raw_csv)

    result = pipeline.run("people.csv")

    expected = [
        {"name": "Ann", "age": "30"},
        {"name": "Bob", "age": "25"},
    ]
    assert json.loads(result) == expected


def test_json_stats_pipeline():
    raw_json = "[10, 20, 30]"
    pipe = JsonStatsPipeline(reader=lambda p: raw_json)

    stats = pipe.run("nums.json")

    assert stats == {"count": 3, "avg": 20.0, "max": 30}

# ---------------------------------------------------------------------------
# Негатив — неверные аргументы / формат данных
# ---------------------------------------------------------------------------

def test_empty_path_raises():
    pipe = CsvToJsonPipeline(reader=lambda p: "")
    with pytest.raises(ValueError):
        pipe.run("")


def test_json_stats_pipeline_wrong_format():
    raw = "{\"a\": 1}"
    pipe = JsonStatsPipeline(reader=lambda p: raw)
    with pytest.raises(TypeError):
        pipe.run("file.json")

# ---------------------------------------------------------------------------
# Мок — чтение файла заменено через patch
# ---------------------------------------------------------------------------

def test_reader_called_once():
    with patch("pattern_impl._read_file", return_value="[]") as fake_read:
        CsvToJsonPipeline().run("dummy.csv")

    fake_read.assert_called_once_with("dummy.csv")