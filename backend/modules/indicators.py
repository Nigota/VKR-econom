import pandas as pd
import numpy as np

# Импорт собственного логгера для ведения логов и установки идентификатора запроса
from backend.logger import get_logger, set_request_id

# Инициализация логгера и установка идентификатора запроса для удобного фильтрования логов
logger = get_logger()


def equal_weighted_index(data: dict) -> dict:
    """
    Считает равновзвешенный индекс (Equal Weighted Index, EWI) по словарю
    вида {date: {ticker: {close, high, low, open, volume}}}.

    Логика:
        - Для каждой даты берём цены закрытия всех тикеров.
        - Находим максимальную цену закрытия среди всех тикеров.
        - Для каждой бумаги считаем "взвешенное значение": close * (max_price / close),
          чтобы у всех акций вклад был одинаковый.
        - Индекс на день = среднее всех "взвешенных" значений.

    Args:
        data (dict): Исторические данные по тикерам.

    Returns:
        dict: {date: index_value} — равновзвешенный индекс по дням.
    """

    def ewi(prices):
        # Определяем максимальную цену закрытия для нормализации
        max_price = max(prices)

        # Рассчитываем "взвешенные" цены: уравниваем вклад каждой акции
        weighted = [price * (max_price / price) for price in prices]

        # Индекс на этот день = среднее всех взвешенных значений
        return sum(weighted) / len(weighted)

    set_request_id("indicator")

    res = {}  # словарь для хранения результата
    logger.info("СЧИТАЮ ИНДИКАТОРЫ")
    # Перебираем все даты в исходных данных
    for date, tickers in data.items():
        if not tickers:
            continue  # пропускаем дни без данных

        # Берём список цен свеч для всех тикеров на эту дату
        opens = [info["open"] for info in tickers.values()]
        highs = [info["high"] for info in tickers.values()]
        closes = [info["close"] for info in tickers.values()]
        lows = [info["low"] for info in tickers.values()]

        # Сохраняем результат для текущей даты
        res[date] = {
            "open": ewi(opens),
            "high": ewi(highs),
            "low": ewi(lows),
            "close": ewi(closes),
        }

    return res


def atr(security: pd.DataFrame, period=1) -> pd.DataFrame:
    set_request_id("indicator")

    if not isinstance(security, pd.DataFrame):
        raise ValueError("Надо использовать pandas.DataFrame")

    prev_close = security["close"].shift(1)
    tr = pd.concat(
        [
            security["high"] - security["low"],
            (security["high"] - prev_close).abs(),
            (security["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    atr = tr.rolling(period, min_periods=1).mean()

    # Определяем направление: +1 если рост, -1 если падение, 0 если без изменений
    direction = (security["close"] - prev_close).apply(
        lambda x: 1 if x > 0 else (-1 if x < 0 else 0)
    )

    atr = pd.DataFrame(
        {
            "begin": security["begin"],
            "atr": atr,
            "atr_%": atr / security["close"] * 100,
            "direction": direction,
        }
    )

    return atr


def correlation(security1, security2, length):
    set_request_id("indicator")
    pass


if __name__ == "__main__":
    from pprint import pprint
    from backend.modules.moex_api import (
        load_available_history_imoex_list_with_prices_to,
        get_kline,
    )

    # проверка индекса
    data = load_available_history_imoex_list_with_prices_to(
        date="2025-09-16", days_back=10
    )

    result = equal_weighted_index(data)
    pprint(result)

    # # проверка корреляции
    # data = get_kline("SBER", "2025-09-10", "2025-09-16")
    # pprint(data)

    # print(atr(data))
