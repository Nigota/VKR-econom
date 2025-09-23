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
    set_request_id("indicator")

    def ewi(prices):
        # Определяем максимальную цену закрытия для нормализации
        max_price = max(prices)

        # Рассчитываем "взвешенные" цены: уравниваем вклад каждой акции
        weighted = [price * (max_price / price) for price in prices]

        # Индекс на этот день = среднее всех взвешенных значений
        return sum(weighted) / len(weighted)

    res = {}  # словарь для хранения результата
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


def atr(security: pd.DataFrame, period: int = 1) -> pd.DataFrame:
    """
    Рассчитывает Average True Range (ATR) для заданного инструмента и определяет направление движения.

    ATR показывает средний диапазон колебаний цены за период.
    Функция также возвращает ATR в процентах и направление движения цены.

    Args:
        security (pd.DataFrame): Датафрейм с историей котировок.
            Обязательные колонки: ['date', 'open', 'high', 'low', 'close', 'volume'].
        period (int, optional): Период для расчета скользящего среднего ATR. По умолчанию 1.

    Returns:
        pd.DataFrame: Датафрейм с колонками:
            - 'date': дата свечи,
            - 'atr': рассчитанный ATR,
            - 'atr_%': ATR в процентах относительно закрытия,
            - 'direction': направление движения цены (+1 рост, -1 падение, 0 без изменений).

    Raises:
        ValueError: Если security не является pandas.DataFrame.
    """
    set_request_id("indicator")

    if not isinstance(security, pd.DataFrame):
        raise ValueError("Надо использовать pandas.DataFrame")

    # Предыдущее закрытие
    prev_close = security["close"].shift(1)

    # Рассчитываем True Range
    tr = pd.concat(
        [
            security["high"] - security["low"],
            (security["high"] - prev_close).abs(),
            (security["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    # ATR: скользящее среднее True Range
    atr = tr.rolling(period, min_periods=1).mean()

    # Определяем направление: +1 если рост, -1 если падение, 0 если без изменений
    direction = (security["close"] - prev_close).apply(lambda x: -1 if x <= 0 else 1)

    # Формируем результат
    atr_df = pd.DataFrame(
        {
            "date": security["date"],
            "atr": atr,
            "atr_%": atr / security["close"] * 100,
            "direction": direction,
        }
    )

    return atr_df


def correlation(df1: pd.DataFrame, df2: pd.DataFrame, window: int = 3) -> pd.DataFrame:
    """
    Рассчитывает скользящую корреляцию двух активов по указанной колонке.

    Args:
        df1 (pd.DataFrame): Первый датафрейм с колонкой column и 'date'.
        df2 (pd.DataFrame): Второй датафрейм с колонкой column и 'date'.
        column (str): Колонка для расчета корреляции (по умолчанию 'atr_%').
        window (int): Окно для скользящей корреляции (количество свечей).

    Returns:
        pd.DataFrame: Датафрейм с колонками:
            - 'date' : даты
            - 'rolling_corr' : скользящая корреляция двух активов
    """
    # Создаем промежуточную колонку для вычисления корреляции
    column = "tmp"
    df1[column] = df1["atr_%"] * df1["direction"]
    df2[column] = df2["atr_%"] * df2["direction"]

    # Объединяем по дате
    merged = pd.merge(
        df1[["date", column]], df2[["date", column]], on="date", suffixes=("_1", "_2")
    )

    # Скользящая корреляция
    merged["corr"] = (
        merged[f"{column}_1"].rolling(window=window).corr(merged[f"{column}_2"])
    )

    # Удаляем колонку, чтобы не менять исходные датафреймы
    df1.drop(columns=[column], inplace=True)
    df2.drop(columns=[column], inplace=True)
    return merged[["date", "corr"]].dropna()


def activity(security: pd.DataFrame, index: pd.DataFrame) -> pd.DataFrame:
    """
    Рассчитывает коэффициент активности между двумя временными рядами на основе ATR (%).

    Args:
        security (pd.DataFrame): Датафрейм с данными бумаги.
                                 Должен содержать колонки 'date' и 'atr_%'.
        index (pd.DataFrame): Датафрейм с данными индекса.
                              Должен содержать колонки 'date' и 'atr_%'.

    Returns:
        pd.DataFrame: Датафрейм с колонками:
                      - 'date': даты из исходных датафреймов
                      - 'activity': отношение ATR (%) бумаги к ATR (%) индекса
    """
    # Объединяем данные по дате, чтобы значения бумаги и индекса совпадали
    merged = pd.merge(
        security[["date", "atr_%"]],  # Выбираем только нужные колонки из security
        index[["date", "atr_%"]],  # Выбираем только нужные колонки из index
        on="date",  # Объединяем по дате
        suffixes=("_sec", "_idx"),  # Добавляем суффиксы, чтобы различать колонки
    )

    # Вычисляем активность как отношение ATR бумаги к ATR индекса
    merged["activity"] = merged["atr_%_sec"] / merged["atr_%_idx"]

    # Возвращаем только дату и вычисленную активность
    return merged[["date", "activity"]]


if __name__ == "__main__":
    from pprint import pprint
    from backend.modules.moex_api import (
        load_available_history_imoex_list_with_prices_to,
        get_kline,
        load_imoex_list,
        top_by_volume,
    )

    # -------------- ПРОВЕРКА ИНДЕКСА -------------------

    # data = load_available_history_imoex_list_with_prices_to(
    #     date="2025-09-18", days_back=10
    # )
    # result = equal_weighted_index(data)
    # pprint(result)

    # -------------- ПРОВЕРКА ATR --------------------

    # проверка работы с акциями
    # data = get_kline("SBER", "2025-09-10", "2025-09-16")
    # data.rename(columns={"begin": "date"}, inplace=True)

    # проверка работы с индексом
    # data = load_available_history_imoex_list_with_prices_to(
    #     date="2025-09-18", days_back=10
    # )
    # data = equal_weighted_index(data)
    # data = pd.DataFrame.from_dict(data, orient="index").reset_index()
    # data.rename(columns={"index": "date"}, inplace=True)
    # data["date"] = pd.to_datetime(data["date"])
    # data = data.sort_values(by="date").reset_index(drop=True)
    # pprint(data)

    # print(atr(data))

    # ---------------- ПРОВЕРКА КОРРЕЛЯЦИИ И АКТИВНОСТИ -----------------

    data_index = load_available_history_imoex_list_with_prices_to(
        date="2025-09-18", days_back=17
    )
    data_index = equal_weighted_index(data_index)
    data_index = pd.DataFrame.from_dict(data_index, orient="index").reset_index()
    data_index.rename(columns={"index": "date"}, inplace=True)
    data_index["date"] = pd.to_datetime(data_index["date"])
    data_index = data_index.sort_values(by="date").reset_index(drop=True)

    data_sec = get_kline("SBER", "2025-09-01", "2025-09-18")
    data_sec.rename(columns={"begin": "date"}, inplace=True)
    data_sec["date"] = pd.to_datetime(data_sec["date"])

    atr_index = atr(data_index)
    atr_sec = atr(data_sec)

    cor = correlation(atr_sec, atr_index, window=5)
    activity = activity(atr_sec, atr_index)

    print(data_index)
    print(data_sec)
    print()
    print(atr_index)
    print(atr_sec)
    print()
    print(cor)
    print()
    print(activity)
