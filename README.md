# JSONPlaceholder ETL Pipeline

## 📋 Описание

Проект реализует:
- Загрузку данных из `jsonplaceholder.typicode.com` (users, posts, comments)
- Сохранение в SQLite с поддержкой повторного запуска без дублирования данных
- Airflow DAG с расписанием запуска
- Полную контейнеризацию через Docker

## 🏗️ Структура проекта

```
Test_4ibbis/
├── dags/                          # Airflow DAGs
│   └── jsonplaceholder_etl_dag.py # Основной DAG
├── scripts/                       # ETL скрипты
│   └── etl_jsonplaceholder.py     # Скрипт загрузки данных
├── data/                          # SQLite база данных
├── logs/                          # Логи Airflow
├── plugins/                       # Airflow плагины
├── requirements.txt               # Python зависимости
├── Dockerfile                     # Образ Airflow
├── docker-compose.yml             # Docker конфигурация
├── .env                           # Переменные окружения
└── README.md                      # Документация
```

## 🚀 Быстрый старт

### Предварительные требования

- Docker Desktop
- Docker Compose

### Запуск проекта

1. **Клонируйте репозиторий**
```bash
cd D:\AI\Test_4ibbis
```

2. **Запустите Docker Compose**
```bash
docker compose up -d
```

3. **Откройте Airflow Web UI**
```
http://localhost:8080
```

Логин: `airflow`  
Пароль: `airflow`

4. **Запустите DAG**
- Включите DAG `jsonplaceholder_etl`
- Он будет запускаться автоматически каждый день (`@daily`)
- Или запустите вручную через Trigger DAG

### Остановка проекта

```bash
docker compose down
```

### Остановка с удалением volumes

```bash
docker compose down -v
```

## 🔧 Ручной запуск ETL скрипта

Без Docker:

```bash
# Установка зависимостей
pip install -r requirements.txt

# Запуск ETL
python scripts/etl_jsonplaceholder.py
```

## 📊 Технологии

- **Apache Airflow 2.10.5** - оркестрация ETL процессов
- **Python 3.11** - язык разработки
- **pandas 2.2.3** - обработка данных
- **SQLite** - хранение данных
- **Docker & Docker Compose** - контейнеризация
- **PostgreSQL** - метаданные Airflow
- **Redis** - брокер сообщений для Celery

## 📝 Конфигурация DAG

- **Dag ID**: `jsonplaceholder_etl`
- **Расписание**: `@daily` (каждый день)
- **Retries**: 2 попытки
- **Retry delay**: 5 минут

## 🗄️ База данных

SQLite база находится в `data/jsonplaceholder.db`

Таблицы:
- `users` - пользователи
- `posts` - посты
- `comments` - комментарии

## 🔍 Логирование

Логи доступны:
- В Airflow Web UI (Task Logs)
- В директории `logs/`
- В консоли при ручном запуске
