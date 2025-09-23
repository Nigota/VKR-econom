import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from backend.modules.moex_api import (
    load_available_history_imoex_list_with_prices_to,
    top_by_volume,
)
from backend.modules.indicators import equal_weighted_index, atr, correlation, activity
from backend.logger import get_logger, set_request_id
from backend.config import MOEX_DATA_WP

logger = get_logger()


def _format_date(date: str) -> datetime:
    """
    Преобразует строку в объект datetime с дополнительными проверками.

    Если дата пустая, возвращается текущая дата.
    Если дата в будущем, она заменяется на сегодняшний день.

    Args:
        date (str): Дата в формате 'YYYY-MM-DD'.

    Returns:
        datetime.datetime: Проверенная и отформатированная дата.

    Raises:
        ValueError: Если аргумент не является строкой.
        ValueError: Если строка не соответствует формату 'YYYY-MM-DD'.
    """
    if not isinstance(date, str):
        raise ValueError("Дата должна быть строкой!")

    if not date:
        return datetime.today()

    try:
        parsed_date = datetime.strptime(date, "%Y-%m-%d")
        return min(parsed_date, datetime.today())
    except ValueError:
        raise ValueError("Неправильный формат даты. Ожидается: YYYY-MM-DD")


def get_tradable_securities(date="", sec_cnt=20, look_back=5, min_act=1, min_corr=0.7):
    set_request_id("indicator")
    logger.info("Начинаю отбор бумаг для торговле...")

    try:
        # Преобразуем входные параметры к datetime и проверяем корректность формата
        date = _format_date(date).strftime("%Y-%m-%d")
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        return []

    # ЭТАП 1: получаем все тикеры и свечи за 10 дней + look_back
    set_request_id("indicator")
    logger.info("ЭТАП 1: получаем все тикеры и свечи за 10 дней + look_back")
    days_back = 10 + look_back
    data_index = load_available_history_imoex_list_with_prices_to(
        date=date, days_back=days_back
    )

    if date not in data_index:
        return []

    # ЭТАП 2: расчитываем сам индекс
    set_request_id("indicator")
    logger.info("ЭТАП 2: расчитываем сам индекс")

    ewi = equal_weighted_index(data_index)

    # ЭТАП 3: отбираем топ бумаг по объему и загружаем их
    set_request_id("indicator")
    logger.info("ЭТАП 3: отбираем топ бумаг по объему")

    top_sec = top_by_volume(date, sec_cnt)

    moex_hist = pd.read_parquet(MOEX_DATA_WP)
    moex_hist.date = pd.to_datetime(moex_hist.date)

    start_dt = pd.to_datetime(date) - timedelta(days=days_back)
    end_dt = pd.to_datetime(date)
    securities = moex_hist[
        (moex_hist.date >= start_dt)
        & (moex_hist.date <= end_dt)
        & moex_hist.ticker.isin(top_sec)
    ].reset_index(drop=True)
    securities.date = pd.to_datetime(securities.date)
    securities = securities.sort_values(by="date").reset_index(drop=True)

    # ЭТАП 4: отбираем из топа только бумаги с хорошей корреляцией и волатильностью
    set_request_id("indicator")
    logger.info(
        "ЭТАП 4: отбираем из топа только бумаги с хорошей корреляцией и волатильностью"
    )

    # Преобразовываем ewi для дальнейшей работы
    ewi_df = pd.DataFrame.from_dict(ewi, orient="index").reset_index()
    ewi_df.rename(columns={"index": "date"}, inplace=True)
    ewi_df.date = pd.to_datetime(ewi_df.date)
    ewi_df = ewi_df.sort_values(by="date").reset_index(drop=True)

    # Расчитываем atr индекса
    index_atr = atr(ewi_df)

    # Отбираем бумаги
    super_top = []
    for sec in top_sec:
        sec_df = securities[securities.ticker == sec].reset_index(drop=True)
        sec_atr = atr(sec_df)
        sec_corr_with_index = correlation(sec_atr, index_atr)
        sec_activity = activity(sec_atr, index_atr)

        cur_corr = sec_corr_with_index[sec_corr_with_index.date == end_dt].iloc[0, 1]
        cur_act = sec_activity[sec_activity.date == end_dt].iloc[0, 1]
        if cur_corr >= min_corr and cur_act >= min_act:
            super_top.append(sec)

    return super_top


if __name__ == "__main__":
    from pprint import pprint

    res = get_tradable_securities(date="2025-09-18")
    pprint(res)
