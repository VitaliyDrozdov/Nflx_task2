import os
from dotenv import load_dotenv
from datetime import datetime

from core.db_config import engine
from core.logging_config import log_to_db
from core.parser import read_data, load_to_db
from core.manage_tables import create_tables

load_dotenv()

CSVPATH = os.getenv("CSVPATH")

DM_TABLENAME = "loan_holiday_info"


def list_files(directory):
    """Возвращает список файлов в указанной директории."""
    try:
        csv_files = [
            f
            for f in os.listdir(directory)
            if os.path.isfile(os.path.join(directory, f))
        ]
        return csv_files
    except FileNotFoundError(f"Директория {directory} не найдена."):
        pass
    except PermissionError(f"Нет доступа к директории {directory}."):
        pass


def main():
    """Загрузка данных из папки с .csv файлами и логирование в БД."""
    create_tables(engine)
    start_time = datetime.now()
    files = list_files(CSVPATH)
    print(files)
    for file in files:
        table_name = file.split(".")[0]
        data = read_data(
            file_path=f"{CSVPATH}/{file}",
            encoding="cp1252",
            delimiter=",",
        )
        load_to_db(
            data=data,
            table_name=f"{table_name}" + "_temp",
            engine=engine,
            schema="rd",
            clean=False,
            dropna=False,
        )
    end_time = datetime.now()
    log_to_db(
        table_name="load_to_rd", start_time=start_time, end_time=end_time
    )


if __name__ == "__main__":
    main()
