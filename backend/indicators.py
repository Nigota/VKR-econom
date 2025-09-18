import pandas as pd
import numpy as np


def equal_weight_index(data: dict) -> dict:
    index_candles = {}

    for date, tickers in data.items():
        if not tickers:
            continue  # пропускаем дни без данных

        closes = [v["close"] for v in tickers.values()]

        index_candles[date] = sum(closes) / len(closes)

    return index_candles


if __name__ == "__main__":
    from pprint import pprint
    from moex_api import load_available_history_imoex_list_with_prices_to

    data = load_available_history_imoex_list_with_prices_to(
        date="2020-09-14", days_back=2
    )

    result = equal_weight_index(data)
    pprint(result)
