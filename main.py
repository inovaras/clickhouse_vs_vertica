import asyncio
import time
import random
from datetime import datetime, timedelta
from uuid import uuid4
import matplotlib.pyplot as plt

import clickhouse_connect
import vertica_python

from config.settings import settings, ClickhouseSettings, VerticaSettings
from config.logging import logger
from models import Event


class ClickhouseLoader:
    def __init__(self, settings: ClickhouseSettings) -> None:
        self.settings = settings
        self.client = clickhouse_connect.get_client(
            host=self.settings.host,
            port=self.settings.port,
            database="default",
            username=self.settings.user,
            password=self.settings.password,
        )
        self.init_clickhouse()  # Создание базы данных и таблицы

    def init_clickhouse(self):
        logger.info("Initializing ClickHouse")
        try:
            # Проверка и создание базы данных
            self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.settings.database}")

            # Переключение на нужную базу
            self.client.command(f"USE {self.settings.database}")

            result = self.client.command(
            f"DROP TABLE IF EXISTS {self.settings.database}.{self.settings.table}"
        )
            logger.info(f"DROP db result: {result}")

            # Создание таблицы
            logger.info(f"Creating table: {self.settings.table} in database: {self.settings.database}")

            self.client.command(
                f"CREATE TABLE IF NOT EXISTS {self.settings.database}.{self.settings.table} ("
                f"event_type String, "
                f"timestamp DateTime64, "
                f"user_id UUID NULL, "
                f"url String NULL"
                f") Engine=MergeTree() ORDER BY timestamp"
            )

            logger.info(f"Table '{self.settings.table}' created successfully in database '{self.settings.database}'.")

        except Exception as e:
            logger.exception("An error occurred during ClickHouse initialization")


    def load_batch(self, event_batch: list[Event]):
        if not event_batch:
            return
        column_names = list(Event.model_fields.keys())
        data = [tuple(event.model_dump().values()) for event in event_batch]
        try:
            result = self.client.insert(
                table="example.events", data=data, column_names=column_names
            )
            logger.info(f"Loaded batch with result {result.summary}")
        except Exception as e:
            logger.exception(f"Error while loading batch into ClickHouse: {e}")

    def read_data(self):
        start_time = time.time()
        query_result = self.client.query(f"SELECT * FROM {self.settings.database}.{self.settings.table}")
        read_time = time.time() - start_time
        rows = query_result.result_rows if hasattr(query_result, 'result_rows') else []
        return read_time


class DatabaseManager:
    def __init__(self, clickhouse_settings: ClickhouseSettings, vertica_settings: VerticaSettings):
        self.vertica_settings = vertica_settings
        self.clickhouse_loader = ClickhouseLoader(clickhouse_settings)
        self.vertica_connection = self.connect_to_vertica()

    def connect_to_vertica(self):
        """Попытка подключиться к Vertica"""
        connection_info = {
            'host': self.vertica_settings.host,
            'port': self.vertica_settings.port,
            'user': self.vertica_settings.user,
            'password': self.vertica_settings.password,
            'database': self.vertica_settings.database,
            'autocommit': True,
        }

        try:
            logger.info(connection_info)
            connection = vertica_python.connect(**connection_info)
            logger.info("Connected to Vertica successfully")
            return connection
        except vertica_python.errors.ConnectionError as e:
            logger.error(f"connect to Vertica failed: {e}")
        raise ConnectionError()

    def create_vertica_table(self):
        logger.info("Creating Vertica table")
        try:
            with self.vertica_connection.cursor() as cursor:
                logger.info(f"Creating table: {settings.vertica.table} in schema: {settings.vertica.vertica_schema}")
                cursor.execute(
                    f"CREATE TABLE IF NOT EXISTS {settings.vertica.vertica_schema}.{settings.vertica.table} ("
                    "event_type VARCHAR(255), "
                    "timestamp TIMESTAMP, "
                    "user_id UUID NULL, "
                    "url VARCHAR(255) NULL)"
                )
                logger.info(f"Table '{settings.vertica.table}' created successfully in schema '{settings.vertica.vertica_schema}'.")

        except Exception as e:
            logger.exception("An error occurred while creating the Vertica table")

    def read_data_from_vertica(self):
        start_time = time.time()
        with self.vertica_connection.cursor() as cur:
            cur.execute(f"SELECT * FROM {settings.vertica.vertica_schema}.{settings.vertica.table}")
            rows = cur.fetchall()
        read_time = time.time() - start_time
        return read_time


def generate_events(num_events: int) -> list[Event]:
    return [
        Event(
            event_type=random.choice(['click', 'view', 'purchase']),
            timestamp=datetime.now() - timedelta(days=random.randint(0, 10)),
            user_id=uuid4() if random.random() > 0.5 else None,
            url=random.choice(['http://example.com', 'http://test.com', None])
        )
        for _ in range(num_events)
    ]

def load_data_to_clickhouse(events: list[Event], loader: ClickhouseLoader):
    start_time = time.time()
    loader.load_batch(events)
    return time.time() - start_time


def load_data_to_vertica(events: list[Event], vertica_loader):
    start_time = time.time()
    with vertica_loader.cursor() as cur:
        data = [(event.event_type, event.timestamp, event.user_id, event.url) for event in events]
        for d in data:
            cur.execute("INSERT INTO public.events VALUES (%s, %s, %s, %s)", d)

        cur.execute("COMMIT")

    return time.time() - start_time


async def main():
    try:
        logger.info("Starting data generation and loading process")

        # Создание таблиц
        db_manager = DatabaseManager(settings.clickhouse, settings.vertica)
        db_manager.clickhouse_loader.init_clickhouse()

        db_manager.create_vertica_table()

        # Генерация данных
        num_events = 1000
        events = generate_events(num_events)

        # Загрузка данных в ClickHouse
        clickhouse_write_time = load_data_to_clickhouse(events, db_manager.clickhouse_loader)

        # Загрузка данных в Vertica
        vertica_write_time = load_data_to_vertica(events, db_manager.vertica_connection)

        # Чтение данных из ClickHouse
        clickhouse_read_time = db_manager.clickhouse_loader.read_data()

        # Чтение данных из Vertica
        vertica_read_time = db_manager.read_data_from_vertica()

        # Вывод времени
        logger.info(f"ClickHouse write time: {clickhouse_write_time:.2f} seconds")
        logger.info(f"Vertica write time: {vertica_write_time:.2f}  seconds")
        logger.info(f"ClickHouse read time: {clickhouse_read_time:.2f} seconds")
        logger.info(f"Vertica read time: {vertica_read_time:.2f} seconds")

        return {
            'clickhouse_write_time': clickhouse_write_time,
            'vertica_write_time': vertica_write_time,
            'clickhouse_read_time': clickhouse_read_time,
            'vertica_read_time':vertica_read_time}

    except Exception:
        logger.exception("Unhandled exception")

def plot_results(clickhouse_write_time, vertica_write_time, clickhouse_read_time, vertica_read_time):
    labels = ['ClickHouse', 'Vertica']
    write_times = [clickhouse_write_time, vertica_write_time]
    read_times = [clickhouse_read_time, vertica_read_time]

    fig, ax = plt.subplots(1, 2, figsize=(12, 6))

    ax[0].bar(labels, write_times, color=['blue', 'green'])
    ax[0].set_title('Write Times')
    ax[0].set_ylabel('Time (seconds)')

    ax[1].bar(labels, read_times, color=['blue', 'green'])
    ax[1].set_title('Read Times')
    ax[1].set_ylabel('Time (seconds)')

    plt.suptitle('Database Performance Comparison')

    # Сохраняем график в файл database_performance_comparison.png
    plt.savefig("database_performance_comparison.png")
    logger.info("Graph saved as 'database_performance_comparison.png'")


if __name__ == "__main__":

    time.sleep(3)
    logger.info("Start script")
    times = asyncio.run(main())
    plot_results(**times)
