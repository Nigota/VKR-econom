import requests
import pandas as pd
import numpy as np
import os
from time import sleep
from datetime import datetime, timedelta

# Импорт собственного логгера для ведения логов и установки идентификатора запроса
from backend.logger import get_logger, set_request_id
from backend.config import MOEX_DATA_WP, MOEX_DATA_L

# Инициализация логгера и установка идентификатора запроса для удобного фильтрования логов
logger = get_logger()


def _check_existing_file(file_path: str) -> bool:
    """
    Проверяет существование файла и при необходимости создает директории.

    Функция проверяет, существует ли файл по указанному пути.
    Если файл отсутствует, автоматически создаются все необходимые директории,
    в которых этот файл должен располагаться. Сам файл при этом не создается.

    Args:
        file_path (str): Путь к файлу.

    Returns:
        bool: True, если файл уже существовал, иначе False.
    """
    is_existing_file = os.path.exists(file_path)

    if not is_existing_file:
        os.makedirs(os.path.dirname(os.path.abspath(file_path)), exist_ok=True)

    return is_existing_file


# Проверяем наличие файла истории c ценами
is_moex_data_wp_file_exists = _check_existing_file(MOEX_DATA_WP)
is_moex_data_list_file_exists = _check_existing_file(MOEX_DATA_L)


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


def _save_moex_list(date: str, date_from: str, tickers: list[str]) -> None:
    """
    Сохраняет список тикеров Московской биржи (MOEX) в файл формата Parquet.

    Функция формирует DataFrame с переданными тикерами и датами,
    объединяет его с уже существующими данными (если они есть),
    удаляет дубликаты и сохраняет результат в файл.

    Args:
        date (str): Дата выгрузки в формате 'YYYY-MM-DD'.
        date_from (str): Дата начала периода данных.
        tickers (list[str]): Список тикеров MOEX.

    Returns:
        None

    Globals:
        is_moex_data_list_file_exists (bool): Флаг, указывающий,
            существует ли уже файл с историей данных.
        MOEX_DATA_L (str): Путь к файлу с историей данных.

    Side Effects:
        - Создаёт или обновляет файл с историей данных MOEX.
        - Записывает отладочную информацию в лог.

    Raises:
        OSError: При ошибках записи файла.
        ValueError: Если входные данные некорректны.
    """
    global is_moex_data_list_file_exists

    df = pd.DataFrame(
        {
            "date": [date] * len(tickers),
            "date_from": [date_from] * len(tickers),
            "ticker": tickers,
        }
    )

    if not is_moex_data_list_file_exists:
        new_moex = df
        is_moex_data_list_file_exists = True
    else:
        old_moex = pd.read_parquet(MOEX_DATA_L)
        new_moex = pd.concat([old_moex, df], ignore_index=True)
        new_moex = new_moex.drop_duplicates(
            subset=["date", "ticker"], keep="first"
        ).dropna()

    new_moex.to_parquet(MOEX_DATA_L, index=False)
    logger.debug(f"Данные сохранены в {MOEX_DATA_L}")


def _save_moex_wp_data(date: str, data: dict) -> None:
    """
    Сохраняет постраничные (WP) данные о тикерах MOEX в Parquet-файл.

    Функция преобразует словарь с данными по тикерам в DataFrame,
    добавляет дату, объединяет результат с уже существующими данными
    (если они есть), удаляет дубликаты и сохраняет итог в Parquet-файл.

    Args:
        date (str): Дата выгрузки в формате 'YYYY-MM-DD'.
        data (dict): Словарь с ключами — тикерами, значениями —
            словари с полями: "open", "high", "low", "close", "volume".

    Returns:
        None

    Globals:
        is_moex_data_wp_file_exists (bool): Флаг, указывающий,
            существует ли файл с историей данных.
        MOEX_DATA_WP (str): Путь к файлу с историей данных.

    Side Effects:
        - Создаёт или обновляет файл Parquet с историей MOEX.
        - Пишет отладочную информацию в лог.

    Raises:
        OSError: При ошибках записи файла.
        ValueError: Если данные не содержат обязательных полей.
    """
    global is_moex_data_wp_file_exists

    # Преобразуем словарь в DataFrame
    df = pd.DataFrame.from_dict(data, orient="index")
    df.reset_index(inplace=True)
    df.rename(columns={"index": "ticker"}, inplace=True)
    df["date"] = date

    # Сохраняем только нужные столбцы
    df = df[["date", "ticker", "open", "high", "low", "close", "volume"]]

    if not is_moex_data_wp_file_exists:
        new_moex = df
        is_moex_data_wp_file_exists = True
    else:
        old_moex = pd.read_parquet(MOEX_DATA_WP)
        new_moex = pd.concat([old_moex, df], ignore_index=True)
        new_moex = new_moex.drop_duplicates(
            subset=["date", "ticker"], keep="first"
        ).dropna()

    new_moex.to_parquet(MOEX_DATA_WP, index=False)
    logger.debug(f"Данные сохранены в {MOEX_DATA_WP}")


def get_index_data(index="IMOEX", date="") -> pd.DataFrame:
    """
    Получает данные по индексу MOEX через API ISS и возвращает их в виде DataFrame.

    Params:
        index (str): Символ индекса на MOEX, например 'IMOEX'.
        date (str): Дата в формате 'YYYY-MM-DD'. Если не задана, берется текущая.

    Returns:
        DataFrame: Таблица с аналитическими данными по индексу.
    """
    set_request_id("moex_api")

    # Формируем URL запроса к MOEX ISS API
    url = f"https://iss.moex.com/iss/statistics/engines/stock/markets/index/analytics/{index}.json"
    all_rows = []  # здесь будем хранить все строки данных
    start = 0  # параметр для пагинации (начало строки)

    while True:
        try:
            # Выполняем GET-запрос к API с пагинацией и таймаутом
            params = {
                "date": date,
                "iss.meta": "off",
                "iss.only": "analytics",
                "start": start,
            }
            r = requests.get(
                url,
                params=params,
                timeout=1000,  # большой таймаут на случай медленного соединения
            )
            r.raise_for_status()  # выбросит исключение, если статус ответа != 200
            data = r.json()  # преобразуем ответ в JSON

            # Извлекаем названия колонок и сами строки данных
            cols = data["analytics"]["columns"]
            rows = data["analytics"]["data"]

            # Если данных больше нет, выходим из цикла
            if not rows:
                break

            # Добавляем новые строки к общему списку
            all_rows.extend(rows)
            start += len(rows)  # увеличиваем смещение для следующей "страницы" данных
            sleep(2)
        except Exception as e:
            # Логируем ошибку и возвращаем пустой DataFrame с нужными колонками
            full_url = f"{url}?{requests.compat.urlencode(params)}"
            logger.error(f"Ошибка запроса: {e}. URL: {full_url}")
            sleep(2)
            return pd.DataFrame(
                {
                    "indexid": [],
                    "tradedate": [],
                    "ticker": [],
                    "shortnames": [],
                    "secids": [],
                    "weight": [],
                    "tradingsession": [],
                    "trade_session_date": [],
                }
            )

    # Конвертируем накопленные данные в DataFrame
    df = pd.DataFrame(data=all_rows, columns=cols)
    return df


def get_kline(security, start_dt, end_dt=None, interval=None) -> pd.DataFrame:
    """
    Функция возвращает датафрейм со свечными данными по одной акции
    за указанный период [start_dt; end_dt] с заданным интервалом.

    По-умолчанию:
      - end_dt = start_dt (если не указан конец периода)
      - interval = "24" (дневные свечи)
    Поддерживаются интервалы:
      - минуты: 1–10
      - 60 -> часовые
      - 24 -> дневные
    """
    set_request_id("moex_api")

    # Если дата конца периода не задана, берём только один день (start_dt)
    end_dt = start_dt if end_dt is None else end_dt
    # По умолчанию берём дневные свечи (interval = "24")
    interval = "24" if interval is None else interval

    # Цикл для отправки нескольких запросов
    try_cnt = 5
    while try_cnt > 0:
        try:
            # Формируем базовый URL для ISS API
            url = f"https://iss.moex.com/iss/engines/stock/markets/shares/securities/{security}/candles.json"
            # Параметры запроса: даты и интервал
            params = {
                "from": start_dt,
                "till": end_dt,
                "interval": interval,
            }
            # Делаем запрос к API (с большим таймаутом на случай долгой загрузки)
            r = requests.get(url, params=params, timeout=1000)
            # Если сервер вернул ошибку (например, 404/500), выбросится исключение
            r.raise_for_status()
            # Преобразуем ответ в JSON
            data = r.json()
            # Если всё сработало корректно, то выходим из цикла
            break

        except Exception as e:
            # Возвращаем пустой датафрейм со стандартными колонками
            try_cnt -= 1
            sleep(2)

    if try_cnt == 0:
        full_url = f"{url}?{requests.compat.urlencode(params)}"
        logger.error(f"Ошибка запроса url: {full_url}")
        return pd.DataFrame(
            data=[], columns=["begin", "open", "high", "low", "close", "volume"]
        )

    # Достаём структуру ответа: список колонок и сами строки данных
    cols = data["candles"]["columns"]
    rows = data["candles"]["data"]

    # Преобразуем результат в DataFrame
    df = pd.DataFrame(rows, columns=cols)

    # Оставляем только основные поля: время начала свечи, цены и объём
    df = df[["begin", "open", "high", "low", "close", "volume"]]
    return df


def load_imoex_list(date="") -> dict:
    """
    Загружает список тикеров, входящих в состав индекса IMOEX на указанную дату
    или ближайшую доступную дату в пределах последних 30 дней.

    Params:
        date (str): Дата в формате 'YYYY-MM-DD'. Если не указана, берется текущая дата.

    Returns:
        dict: {дата: [список тикеров]} на дату с доступными данными.
    """
    global is_moex_data_list_file_exists

    set_request_id("moex_api")

    try:
        # Преобразуем входные параметры к datetime и проверяем корректность формата
        start_date = date
        date = _format_date(date)
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        {date.strftime("%Y-%m-%d"): []}

    # Если дата = сегодня, берём предыдущий день,
    # т.к. состав индекса доступен только за закрытые сессии
    if date == datetime.today():
        date = date - timedelta(days=1)

    # Подгружаем локальные данные, если есть
    if is_moex_data_list_file_exists:
        df = pd.read_parquet(MOEX_DATA_L)

    # Максимальное количество попыток найти данные (до 30 дней назад)
    try_cnt = 30
    imoex_index = []

    # Пытаемся найти состав индекса, двигаясь назад по дням, пока не найдём данные
    while try_cnt > 0 and len(imoex_index) == 0:
        if (
            is_moex_data_list_file_exists
            and (df["date"] == date.strftime("%Y-%m-%d")).any()
        ):
            imoex_index = df[df["date"] == date.strftime("%Y-%m-%d")][
                "ticker"
            ].to_list()
        else:
            # Загружаем данные по индексу через функцию get_index_data
            imoex_index = get_index_data("IMOEX", date.strftime("%Y-%m-%d"))
            # Извлекаем только колонку 'ticker' в виде списка
            imoex_index = imoex_index["ticker"].to_list()

        # Если данные отсутствуют, идем на день назад и уменьшаем счетчик попыток
        if len(imoex_index) == 0:
            date = date - timedelta(days=1)
            try_cnt -= 1

    _save_moex_list(start_date, date.strftime("%Y-%m-%d"), imoex_index)

    return {date.strftime("%Y-%m-%d"): imoex_index}


def load_imoex_list_with_prices(date="") -> dict:
    """
    Загружает список тикеров, входящих в состав индекса IMOEX на указанную дату,
    и подгружает для них свечные данные (OHLCV).

    Params:
        date (str): Дата в формате 'YYYY-MM-DD'. Если не указана, берется текущая дата.

    Returns:
        dict: {дата: {тикер: {'open', 'high', 'low', 'close', 'volume'}}}
    """
    global is_moex_data_wp_file_exists

    set_request_id("moex_api")
    logger.info("--------------------------------------------")

    date = _format_date(date).strftime("%Y-%m-%d")

    logger.info(f"Начинаю сбор индекса с ценами на {date}...")

    # Сначала получаем состав индекса на нужную дату
    logger.info("Загружаю состав индекса...")
    imoex_index = load_imoex_list(date)

    # Получаем первый ключ словаря — реальная дата индекса (может отличаться от указанной)
    last_date = next(iter(imoex_index))
    # Список тикеров на эту дату
    imoex_index = imoex_index[last_date]

    logger.info(f"Индекс взят за {last_date} - {len(imoex_index)} акций")
    logger.info("Подгружаю цены...")

    # Словарь для хранения данных о тикерах с их свечными данными
    imoex_index_with_prices = {}

    # Подгружаем локальные данные, если есть
    if is_moex_data_wp_file_exists:
        df = pd.read_parquet(MOEX_DATA_WP)

    # Проходим по каждому тикеру
    for security in imoex_index:
        if (
            is_moex_data_wp_file_exists
            and ((df["date"] == date) & (df["ticker"] == security)).any()
        ):
            # Если есть сохраненные данные, то грузим оттуда
            security_data = df[(df["date"] == date) & (df["ticker"] == security)].copy()
            security_data.drop(columns=["ticker"], inplace=True)
        else:
            # Загружаем данные свечей для тикера на дату date c через API
            security_data = get_kline(security=security, start_dt=date)
            security_data.rename(columns={"begin": "date"}, inplace=True)

        # Если данные есть, добавляем в итоговый словарь
        if not security_data.empty:
            # Убираем колонку 'begin' и преобразуем в словарь (одна запись)
            imoex_index_with_prices[security] = security_data.drop(
                columns=["date"]
            ).to_dict(orient="records")[0]

    res = {date: imoex_index_with_prices}

    # Сохраняем данные, чтобы повторно не запрашивать, если данные не пустые
    if len(imoex_index_with_prices) != 0:
        _save_moex_wp_data(date, imoex_index_with_prices)

    logger.info(f"Сбор индекса завершен - {len(imoex_index_with_prices)} акций!")
    logger.info("--------------------------------------------")

    # Возвращаем словарь с датой и данными всех тикеров
    return res


def load_history_imoex_list_with_prices(start_date="", end_date="") -> dict:
    """
    Загружает данные по составу индекса IMOEX и свечным данным всех его тикеров
    за период между start_date и end_date.

    Params:
        start_date (str): Начальная дата в формате 'YYYY-MM-DD'.
        end_date (str): Конечная дата в формате 'YYYY-MM-DD'. Если не указана —
                        загружается только 1 день (аналог load_imoex_list_with_prices).

    Returns:
        dict: {дата: {тикер: {open, high, low, close, volume}}}
    """
    set_request_id("moex_api")

    try:
        # Преобразуем входные параметры к datetime и проверяем корректность формата
        start_date = _format_date(start_date)
        end_date = _format_date(end_date)
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        # Возвращаем пустую структуру с сегодняшней датой
        return {datetime.today().strftime("%Y-%m-%d"): {}}

    if start_date == end_date:
        return load_imoex_list_with_prices(start_date.strftime("%Y-%m-%d"))

    # Логируем диапазон дат
    logger.info(
        f"Начинаю сбор истории индекса с ценами с {start_date.strftime('%Y-%m-%d')} по {end_date.strftime('%Y-%m-%d')}"
    )

    res = {}
    # Перебираем все даты в диапазоне
    while start_date <= end_date:
        # Загружаем данные за конкретную дату
        tmp = load_imoex_list_with_prices(start_date.strftime("%Y-%m-%d"))
        # Объединяем словари
        res = res | tmp

        # Переходим к следующему дню
        start_date += timedelta(days=1)

    logger.info("Сбор истории индекса завершен!")
    return res


def load_available_history_imoex_list_with_prices_to(date="", days_back=0) -> dict:
    """
    Загружает исторические данные по составу индекса IMOEX и свечным данным всех его тикеров
    на указанную дату и заданное количество торговых дней назад.

    Params:
        date (str): Дата в формате 'YYYY-MM-DD'. Если пустая строка — берется текущая дата.
        days_back (int): Количество торговых дней, которые нужно захватить назад от указанной даты.

    Returns:
        dict: {дата: {тикер: {open, high, low, close, volume}}}
    """
    set_request_id("moex_api")

    try:
        # Преобразуем входной параметр в datetime и проверяем его корректность
        date = _format_date(date)
    except Exception as e:
        # Если формат некорректный — логируем ошибку и возвращаем пустой результат
        logger.error(f"Ошибка: {e}")
        return {datetime.today().strftime("%Y-%m-%d"): {}}

    # Логируем, что начинаем сбор данных
    logger.info(
        f"Начинаю сбор истории индекса с ценами на {date.strftime('%Y-%m-%d')} "
        f"за {days_back + 1} торговых сессий"
    )

    res = {}
    # Максимальное количество попыток поиска торговых дней —
    # запас в 30 календарных дней + нужное количество торговых сессий
    try_cnt = 30 + days_back

    # Ищем данные до тех пор, пока не соберем достаточное количество торговых сессий
    while len(res) <= days_back and try_cnt > 0:
        # Загружаем состав индекса и цены на текущую дату
        tmp = load_imoex_list_with_prices(date.strftime("%Y-%m-%d"))

        # Проверяем, что данные по текущей дате не пустые
        if len(tmp[date.strftime("%Y-%m-%d")]) != 0:
            # Если данные есть — добавляем их в общий результат
            res = res | tmp
        else:
            # Если данных нет (например, выходной/праздник) — логируем и пробуем предыдущий день
            logger.info("Сессия пустая, беру другую...")

        # Переходим на 1 календарный день назад
        date = date - timedelta(days=1)
        try_cnt -= 1

    logger.info("Сбор истории индекса завершен!")
    return res


def top_by_volume(date, days_back=0, top_cnt=None):
    try:
        # Преобразуем входной параметр в datetime и проверяем его корректность
        date = _format_date(date)
        date = date.strftime("%Y-%m-%d")

        if days_back < 0:
            raise Exception("Некорректное число дней назад")
    except Exception as e:
        # Если формат некорректный — логируем ошибку и возвращаем пустой результат
        logger.error(f"Ошибка: {e}")
        return {datetime.today().strftime("%Y-%m-%d"): {}}

    data_index = load_available_history_imoex_list_with_prices_to(
        date=date, days_back=3
    )
    securities = {}
    for cur_date in data_index:
        for k, v in data_index[cur_date].items():
            securities[k] = securities.get(k, 0) + v["volume"]
    securities = [[k, v] for k, v in securities.items()]
    securities.sort(key=lambda x: x[1], reverse=True)

    if top_cnt is None:
        top_cnt = len(securities)
    top_cnt = min(top_cnt, len(securities))

    return [sec[0] for sec in securities[:top_cnt]]


if __name__ == "__main__":
    from pprint import pprint

    # Пример вызова: получить тикеры индекса на конкретную дату
    # res = load_history_imoex_list_with_prices()
    # pprint(get_kline("SBER", "2025-09-15"))
    # print(is_moex_data_list_file_exists)
    # res = load_imoex_list(date="2020-09-12")
    # print(pd.read_parquet(MOEX_DATA_L))
    # pprint(res)
    # res = load_available_history_imoex_list_with_prices_to(
    #     date="2025-09-18", days_back=6
    # )
    # print(pd.read_parquet(MOEX_DATA_WP))
    # print(pd.read_parquet(MOEX_DATA_L))
    # pprint(res)
    # _save_moex_data(res)
    # print()
    # pprint(get_index_data(date="2025-09-11"))
    # print()

    # -------------- ПРОВЕРКА ПОЛУЧЕНИЯ ТОПА -------------
    # date = "2025-09-17"
    # days_back = 4
    # data_index = load_available_history_imoex_list_with_prices_to(
    #     date=date, days_back=days_back
    # )
    # pprint(data_index)

    # res = top_by_volume(date, 4, 10)
    # pprint(res)
