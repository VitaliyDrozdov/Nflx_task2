from sqlalchemy import (
    Column,
    Interval,
    Integer,
    MetaData,
    String,
    TIMESTAMP,
)
from sqlalchemy.ext.declarative import declarative_base

from .db_config import LOG_SCHEMA
from .logging_config import logger

Base = declarative_base()
metadata = MetaData()


class ETLLog(Base):
    __tablename__ = "etl_log"
    __table_args__ = {"schema": LOG_SCHEMA}

    id = Column(Integer, primary_key=True)
    table_name = Column(String(255), nullable=False)
    start_time = Column(TIMESTAMP, nullable=False)
    end_time = Column(TIMESTAMP, nullable=False)
    duration = Column(Interval)


def create_tables(engine):
    """Создание таблиц в БД.
    Args:
        engine: соединение с БД.
    """
    try:
        logger.info("Создание таблиц.")
        Base.metadata.create_all(engine, checkfirst=True)
        metadata.create_all(engine, checkfirst=True)
        logger.info("Таблицы созданы.")
    except Exception as err:
        logger.error(f"Ошибка при создании таблиц: {err}")
    return Base.metadata, metadata
