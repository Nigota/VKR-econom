"""
Универсальный логгер для Python.

Особенности:
- Единая точка входа: import get_logger / setup_logging из этого файла.
- Консоль (опционально через LOG_TO_CONSOLE) + TimedRotatingFileHandler (ротация по времени).
- Формат: читаемый текст.
- Гибкая конфигурация через .config.py.
- request_id через contextvars для корреляции логов между вызовами.
- Защита от повторной инициализации (без дублей хендлеров при многократном импорте).

Файл .config.py должен содержать, например:
LOG_LEVEL = "DEBUG"
LOG_FILE = "logs/app.log"
LOG_ROTATE_WHEN = "midnight"
LOG_BACKUP_COUNT = 7
LOG_TO_CONSOLE = True
TIMEZONE = "Europe/Moscow"
"""
from __future__ import annotations

import logging
import os
import sys
import contextvars
from logging.handlers import TimedRotatingFileHandler
from typing import Optional, Callable, Any
from datetime import datetime
import pytz

from backend.config import *

# ========= Контекст =========
_request_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("request_id", default="-")


def set_request_id(request_id: str) -> None:
    """Установить request_id в контекст (отобразится в каждом сообщении лога)."""
    _request_id_var.set(str(request_id))


class _ContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:  # type: ignore[override]
        record.request_id = _request_id_var.get("-")
        record.levelshort = (record.levelname or "").upper()[0:1]
        return True


# ========= Форматтер =========
_DEFAULT_TEXT_FORMAT = (
    "%(asctime)s | %(levelname)-8s | rid=%(request_id)s | %(message)s"
)
_DEFAULT_DATEFMT = "%Y-%m-%dT%H:%M:%S%z"

class TZFormatter(logging.Formatter):
    def __init__(self, fmt: str, datefmt: Optional[str] = None, tz: Optional[str] = None):
        super().__init__(fmt, datefmt)
        self.tz = pytz.timezone(tz) if tz else None

    def formatTime(self, record: logging.LogRecord, datefmt: Optional[str] = None) -> str:  # type: ignore[override]
        dt = datetime.fromtimestamp(record.created)
        if self.tz:
            dt = dt.astimezone(self.tz)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.isoformat()


# ========= Инициализация =========
_configured: bool = False


def _str_to_level(level: Optional[str]) -> int:
    mapping = {
        "NOTSET": logging.NOTSET,
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }
    if isinstance(level, str):
        return mapping.get(level.upper(), logging.INFO)
    if isinstance(level, int):
        return level
    return logging.INFO


def setup_logging(
    *,
    level: Optional[str | int] = None,
    log_file: Optional[str] = None,
    rotate_when: Optional[str] = None,
    backup_count: Optional[int] = None,
    log_to_console: Optional[bool] = None,
    propagate_root: bool = False,
) -> None:
    global _configured
    if _configured:
        return

    # Значения из конфига
    level = _str_to_level(LOG_LEVEL) # type: ignore
    log_file = LOG_PATH
    rotate_when = "midnight"
    backup_count = LOG_BACKUP_COUNT
    log_to_console = LOG_TO_CONSOLE
    tz = TIMEZONE

    root = logging.getLogger()
    root.setLevel(level)
    root.propagate = propagate_root

    ctx_filter = _ContextFilter()
    formatter = TZFormatter(_DEFAULT_TEXT_FORMAT, datefmt=_DEFAULT_DATEFMT, tz=tz)

    # Консольный хендлер (опционально)
    if log_to_console:
        console = logging.StreamHandler(sys.stdout)
        console.addFilter(ctx_filter)
        console.setFormatter(formatter)
        root.addHandler(console)

    # Файловый хендлер (опционально)
    if log_file:
        os.makedirs(os.path.dirname(os.path.abspath(log_file)), exist_ok=True)
        file_handler = TimedRotatingFileHandler(
            log_file, when=rotate_when, interval=1, backupCount=backup_count, encoding="utf-8" # type: ignore
        )
        file_handler.addFilter(ctx_filter)
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)

    _configured = True


def get_logger(name: Optional[str] = None) -> logging.Logger:
    global _configured
    if not _configured:
        setup_logging()
    return logging.getLogger(name or "app")


# ========= Утилиты =========

def log_exceptions(logger: Optional[logging.Logger] = None) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    log = logger or get_logger(__name__)

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return fn(*args, **kwargs)
            except Exception:
                log.exception("Unhandled exception in %s", fn.__name__)
                raise
        return wrapper

    return decorator


# ========= Пример использования =========
if __name__ == "__main__":
    setup_logging()

    set_request_id("REQ-123")
    log = get_logger(__name__)

    log.debug("Отладочное сообщение с данными", extra={"user_id": 42})
    log.info("Приложение запущено")
    try:
        1 / 0
    except ZeroDivisionError:
        log.exception("Ошибка при делении")

    @log_exceptions(log)
    def boom():
        raise RuntimeError("Ой!")

    try:
        boom()
    except RuntimeError:
        pass