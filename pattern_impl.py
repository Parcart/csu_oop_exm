import abc
import csv
import io
import json
from typing import Iterable, List, Dict, Any, Callable

# ---------------------------------------------------------------------------
# Вспомогательная функция — I/O, будет замокана в тестах
# ---------------------------------------------------------------------------

def _read_file(path: str) -> str:  # pragma: no cover
    """Просто читает текстовый файл полностью."""
    with open(path, "r", encoding="utf‑8") as fh:
        return fh.read()

# ---------------------------------------------------------------------------
# Абстрактный шаблон
# ---------------------------------------------------------------------------
class DataPipeline(abc.ABC):
    """Определяет последовательность шагов ETL‑процесса."""

    def __init__(self, reader: Callable[[str], str] | None = None):
        self._reader = reader or _read_file

    # ——— Шаблонный метод ——————————————————————————————
    def run(self, source_path: str) -> Any:
        if not source_path:
            raise ValueError("source_path должен быть непустой строкой")

        raw = self.read(source_path)
        rows = self.parse(raw)
        rows = self.transform(rows)  # опционально
        return self.write(rows)

    # —— Хуки, переопределяемые в подклассах ——————————
    def read(self, path: str) -> str:
        """По‑умолчанию читаем файл через _reader."""
        return self._reader(path)

    @abc.abstractmethod
    def parse(self, raw: str) -> Iterable[dict]:
        """Преобразует сырой текст в записи."""

    def transform(self, rows: Iterable[dict]) -> Iterable[dict]:  # noqa: D401
        """Может изменить записи (по‑умолчанию — как есть)."""
        return rows

    @abc.abstractmethod
    def write(self, rows: Iterable[dict]):
        """Возвращает результат (записывает или сериализует)."""

# ---------------------------------------------------------------------------
# Конкретные реализации
# ---------------------------------------------------------------------------
class CsvToJsonPipeline(DataPipeline):
    """Читает CSV → отдаёт JSON‑строку."""

    def parse(self, raw: str) -> List[dict]:
        reader = csv.DictReader(io.StringIO(raw))
        return list(reader)

    def write(self, rows: Iterable[dict]) -> str:
        return json.dumps(list(rows), ensure_ascii=False, indent=2)

class JsonStatsPipeline(DataPipeline):
    """Читает JSON‑массив чисел → возвращает статистику."""

    def parse(self, raw: str) -> List[int]:
        data = json.loads(raw)
        if not isinstance(data, list):
            raise TypeError("Ожидался JSON‑массив")
        return data

    def transform(self, rows: Iterable[int]) -> Dict[str, float]:
        nums = list(rows)
        return {
            "count": len(nums),
            "avg": (sum(nums) / len(nums)) if nums else 0,
            "max": max(nums) if nums else None,
        }

    def write(self, rows: Dict[str, float]):
        return rows  # просто вернуть словарь