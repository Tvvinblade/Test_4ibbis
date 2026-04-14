"""
ETL скрипт для загрузки данных из JSONPlaceholder в SQLite
Поддерживает повторный запуск без дублирования данных
"""

import requests
import pandas as pd
import sqlite3
import logging
import os
from typing import List, Dict, Any

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Константы
BASE_URL = "https://jsonplaceholder.typicode.com"
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'jsonplaceholder.db')
ENDPOINTS = ['users', 'posts', 'comments']


def fetch_data(endpoint: str) -> List[Dict[str, Any]]:
    """
    Загружает данные из API JSONPlaceholder
    
    Args:
        endpoint: имя эндпоинта (users, posts, comments)
        
    Returns:
        List[Dict]: список записей в формате JSON
    """
    url = f"{BASE_URL}/{endpoint}"
    logger.info(f"Загрузка данных из {url}")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Загружено {len(data)} записей из {endpoint}")
        return data
    except requests.RequestException as e:
        logger.error(f"Ошибка при загрузке {endpoint}: {e}")
        raise


def transform_data(data: List[Dict[str, Any]], endpoint: str) -> pd.DataFrame:
    """
    Преобразует данные в DataFrame с помощью pandas
    
    Args:
        data: список записей
        endpoint: имя эндпоинта
        
    Returns:
        pd.DataFrame: преобразованные данные
    """
    logger.info(f"Трансформация данных для {endpoint}")
    df = pd.DataFrame(data)
    logger.info(f"Создан DataFrame: {df.shape[0]} строк, {df.shape[1]} колонок")
    return df


def save_to_sqlite(df: pd.DataFrame, table_name: str, db_path: str):
    """
    Сохраняет DataFrame в SQLite без дублирования данных
    
    Args:
        df: DataFrame для сохранения
        table_name: имя таблицы
        db_path: путь к базе данных
    """
    logger.info(f"Сохранение данных в таблицу {table_name}")
    
    # Создаем директорию для БД если не существует
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    
    try:
        # Проверяем существование таблицы
        table_exists = pd.read_sql_query(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'",
            conn
        )
        
        if table_exists.empty:
            # Таблица не существует - создаем
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            logger.info(f"Создана новая таблица {table_name} с {len(df)} записями")
        else:
            # Таблица существует - получаем существующие ID
            existing_ids = pd.read_sql_query(
                f"SELECT id FROM {table_name}",
                conn
            )['id'].tolist()
            
            # Фильтруем новые записи (исключаем дубликаты)
            new_records = df[~df['id'].isin(existing_ids)]
            
            if len(new_records) > 0:
                new_records.to_sql(table_name, conn, if_exists='append', index=False)
                logger.info(f"Добавлено {len(new_records)} новых записей в {table_name}")
            else:
                logger.info(f"Нет новых записей для {table_name} (все данные уже существуют)")
        
        conn.commit()
        
    except Exception as e:
        logger.error(f"Ошибка при сохранении в {table_name}: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


def run_etl():
    """
    Запускает полный ETL процесс для всех эндпоинтов
    """
    logger.info("=" * 50)
    logger.info("Запуск ETL процесса")
    logger.info("=" * 50)
    
    for endpoint in ENDPOINTS:
        try:
            # Extract
            data = fetch_data(endpoint)
            
            # Transform
            df = transform_data(data, endpoint)
            
            # Load
            save_to_sqlite(df, endpoint, DB_PATH)
            
            logger.info(f"✓ ETL завершен для {endpoint}")
            
        except Exception as e:
            logger.error(f"✗ Ошибка ETL для {endpoint}: {e}")
            raise
    
    logger.info("=" * 50)
    logger.info("ETL процесс успешно завершен")
    logger.info("=" * 50)


if __name__ == "__main__":
    run_etl()
