# Finance Project

Проект для сбора, хранения и аналитической обработки рыночных данных. Пайплайн автоматизирован через **Apache Airflow**, сырые данные лежат в **PostgreSQL**, трансформации планируются в **dbt**, дашборды — в **Metabase**.

## Возможности

- **Ежедневная загрузка котировок MOEX** (API Algopack / `moexalgo`) → схема `stock_market`, таблица `prices_moex`.
- **Ежедневная загрузка данных Yahoo Finance**: акции, криптовалюты, фиатные валюты → таблица `prices_yfinance`.
- **Инкрементальная загрузка**: в БД попадают только новые даты; пропуски по календарю (выходные) заполняются forward/back fill.
- **Оркестрация dbt**: отдельный DAG ждёт успешного завершения загрузчиков и запускает `dbt run` и `dbt test`.
- **Локальный стек в Docker**: Airflow (CeleryExecutor), PostgreSQL, Redis, Metabase, контейнер dbt.

## Архитектура

```
┌─────────────────┐     ┌─────────────────┐
│  MOEX API       │     │  Yahoo Finance  │
│  (moexalgo)     │     │  (yfinance)     │
└────────┬────────┘     └────────┬────────┘
         │                       │
         ▼                       ▼
┌────────────────────────────────────────────┐
│           Apache Airflow (DAGs)            │
│  daily_moex_data_loader  │  daily_nyse_*   │
│         08:00 MSK daily                    │
└────────────────────┬───────────────────────┘
                     ▼
         ┌───────────────────────┐
         │  PostgreSQL (внешняя)  │
         │  schema: stock_market  │
         │  prices_moex           │
         │  prices_yfinance       │
         └───────────┬────────────┘
                     ▼
         ┌───────────────────────┐
         │  dbt (analytics/)      │
         │  run_dbt_after_sources │
         │         09:00 MSK      │
         └───────────┬────────────┘
                     ▼
         ┌───────────────────────┐
         │  Metabase (:3000)     │
         └───────────────────────┘
```

> **Примечание.** В `docker-compose` поднимается отдельный PostgreSQL для метаданных Airflow. Хранилище рыночных данных задаётся переменной `DATABASE_URL` (обычно внешняя БД).

## Стек технологий

| Компонент | Версия / инструмент |
|-----------|---------------------|
| Оркестрация | Apache Airflow 3.1.0 (CeleryExecutor) |
| Загрузка MOEX | [moexalgo](https://github.com/moexalgo/moexalgo) |
| Загрузка US/crypto/FX | [yfinance](https://github.com/ranaroussi/yfinance) |
| Трансформации | dbt-core + dbt-postgres 1.9.0 |
| Визуализация | Metabase |
| БД | PostgreSQL |
| Контейнеризация | Docker Compose |

## DAG-и Airflow

| DAG ID | Расписание | Описание |
|--------|------------|----------|
| `daily_moex_data_loader` | `0 8 * * *` (MSK) | Загрузка дневных свечей MOEX в `stock_market.prices_moex` |
| `daily_nyse_data_loader` | `0 8 * * *` (MSK) | Загрузка котировок yfinance в `stock_market.prices_yfinance` |
| `run_dbt_after_sources` | `0 9 * * *` (MSK) | Ожидание обоих загрузчиков → `dbt run` → `dbt test` |

## Требования

- Docker и Docker Compose
- Рекомендуется ≥ 4 GB RAM и 2 CPU для Airflow (см. предупреждения в `docker-compose`)
- Токен API Московской биржи (Algopack) для MOEX
- Доступная PostgreSQL-база со схемой `stock_market` и таблицами для загрузки

## Быстрый старт

### 1. Переменные окружения

Скопируйте шаблон и заполните значения:

```bash
cp .env.example .env
```

| Переменная | Назначение |
|------------|------------|
| `DATABASE_URL` | Подключение SQLAlchemy к БД с рыночными данными (используют загрузчики Airflow) |
| `MOEX_API_TOKEN` | Токен API Algopack MOEX |
| `DB_HOST`, `DB_USER`, `DB_PASS`, `DB_NAME` | Параметры PostgreSQL для dbt (`analytics/profiles.yml`) |

Переменные из `.env` прокидываются в `docker-compose.yaml` и в профиль dbt.

Для dbt создайте профиль из примера:

```bash
cp analytics/profiles.yml.example analytics/profiles.yml
```

> Файлы `.env` и `analytics/profiles.yml` в `.gitignore` — не коммитьте секреты.

### 2. Запуск инфраструктуры

```bash
docker compose up --build
```

После старта:

- **Airflow UI**: http://localhost:8080 (логин/пароль по умолчанию: `airflow` / `airflow`)
- **Metabase**: http://localhost:3000

### 3. Включение DAG-ов

В UI Airflow включите (unpause) DAG-и загрузки и оркестрации dbt. По умолчанию DAG-и создаются на паузе (`DAGS_ARE_PAUSED_AT_CREATION`).

### 4. Ручной запуск dbt (опционально)

```bash
docker compose run --rm dbt run
docker compose run --rm dbt test
```

## dbt

Проект `analytics` объявляет источники в `models/sources.yaml`:

- `stock_market.prices_moex`
- `stock_market.prices_yfinance`
- `stock_market.instruments`

## Зависимости Python (образ Airflow)

Основные пакеты из `requirements.txt`: `pandas`, `sqlalchemy`, `psycopg2-binary`, `moexalgo`, `yfinance`, `python-dotenv`, провайдеры Airflow для PostgreSQL и Docker.
