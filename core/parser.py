import pandas as pd

from .db_config import SCHEMA
from .logging_config import logger


def read_data(file_path, encoding="utf-8", delimiter=";"):
    """Функция загруки данных из csv.
    Args:
        file_path: dataset.
        encoding (str, optional): Кодировка. Defaults to "utf-8".
        delimeter (str, optional): Разделитель.Defaults to ";".
    """
    try:
        logger.info(f"Чтение данных из файла: {file_path}")
        data = pd.read_csv(
            file_path,
            parse_dates=True,
            delimiter=delimiter,
            encoding=encoding,
        )
        return data
    except Exception as err:
        logger.error(f"Ошибка при чтении данных из: {file_path}; {err}")


def clean_data(data, dropna=True):
    """Функция для очистки данных.
    Args:
        data: dataset.
        dropna (bool): Удалять ли строки с Null значениями.
    """

    columns_to_check = [
        col for col in data.columns if col not in ["product_rk", "deal_rk"]
    ]
    logger.info(f"cols to clean: {columns_to_check}")
    data = data.drop_duplicates(subset=columns_to_check)
    if dropna:
        data = data.dropna(how="all", subset=columns_to_check)

    return data


def load_to_db(
    data, table_name, engine, schema=SCHEMA, clean=True, dropna=True
):
    """Загрузка данных в БД.

    Args:
        data: dataset для загрузки.
        table_name (str): наименование таблицы.
        engine: соединение с БД. Например, через SQLAlchemy.
        schema (str, optional): Наименование схемы в БД. Defaults to SCHEMA.
        clean (bool, optional): Очистка данных. Defaults to True.
        dropna (bool, optional): Удалять ли строки с Null значениями.
    """
    try:
        if clean:
            cleaned_data = clean_data(data, dropna=dropna)
        else:
            cleaned_data = data

        cleaned_data.to_sql(
            table_name,
            engine,
            schema=schema,
            if_exists="replace",
            index=False,
        )
        logger.info("Данные загружены в БД.")

    except Exception as err:
        logger.error(
            (f"\nОшибка при загрузке данных в таблицу {table_name}: {err}\n")
        )
