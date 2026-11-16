# Finance Project

Проект для сбора, хранения и аналитической обработки рыночных данных. Пайплайн автоматизирован через **Apache Airflow**, сырые данные лежат в **PostgreSQL**, трансформации планируются в **dbt**, дашборды — в **Metabase**.

## Возможности

- **Ежедневная загрузка котировок MOEX** (API Algopack / `moexalgo`) → схема `stock_market`, таблица `prices_moex`.
- **Ежедневная загрузка данных Yahoo Finance**: акции, криптовалюты, фиатные валюты → таблица `prices_yfinance`.
- **Инкрементальная загрузка по logical_date**: каждый запуск грузит окно `[data_interval_start − LOADER_LOOKBACK_DAYS, data_interval_end)`, запись идемпотентна (`INSERT ... ON CONFLICT DO NOTHING`), поэтому повторные и параллельные запуски не создают дублей, а пропущенные дни догоняются через `catchup`.
- **Оркестрация dbt**: отдельный DAG ждёт успешного завершения загрузчиков и запускает `dbt run` и `dbt test` (dbt установлен в образ Airflow).
- **Локальный стек в Docker**: Airflow (CeleryExecutor), PostgreSQL с метаданными Airflow, PostgreSQL с рыночными данными (`market-postgres`), Redis, Metabase.

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
         ┌──────────────────────────────┐
         │  PostgreSQL market-postgres  │
         │  schema: stock_market        │
         │  prices_moex                 │
         │  prices_yfinance             │
         │  instruments                 │
         └───────────┬──────────────────┘
                     ▼
         ┌────────────────────────────────┐
         │  dbt (в образе Airflow)        │
         │  run_dbt_after_sources         │
         │  09:00 MSK                     │
         └───────────┬────────────────────┘
                     ▼
         ┌───────────────────────┐
         │  Metabase (:3000)     │
         └───────────────────────┘
```

> **Примечание.** В `docker-compose` поднимаются две независимые базы: `postgres` — метаданные Airflow (внутренняя, наружу не публикуется) и `market-postgres` — рыночные данные со схемой `stock_market` (порт `5433` на хосте). Схема и таблицы создаются bootstrap-скриптами из `infra/initdb`.

## Стек технологий

| Компонент | Версия / инструмент |
|-----------|---------------------|
| Оркестрация | Apache Airflow 3.1.0 (CeleryExecutor) |
| Загрузка MOEX | [moexalgo](https://github.com/moexalgo/moexalgo) |
| Загрузка US/crypto/FX | [yfinance](https://github.com/ranaroussi/yfinance) |
| Трансформации | dbt-core + dbt-postgres 1.9.x (внутри образа Airflow) |
| Визуализация | Metabase |
| БД | PostgreSQL 16 (метаданные Airflow + `market-postgres` с рыночными данными) |
| Контейнеризация | Docker Compose |

## Структура репозитория

```text
airflow_setup/
├── dags/
│   ├── moex_dag/dag.py       # DAG загрузки MOEX
│   ├── yfinance_dag/dag.py   # DAG загрузки Yahoo Finance
│   └── dbt_dag/dag.py        # DAG-оркестратор dbt
├── config/airflow.cfg
└── logs/

etl/                          # ETL-логика: импортируется DAG-ами и тестами
├── config.py                 # тикеры, lookback, начало истории
├── dates.py                  # окно загрузки, заполнение пропусков, дедупликация
├── db.py                     # engine, последние даты, идемпотентный upsert
├── yfinance_source.py        # загрузка Yahoo Finance
├── moex_source.py            # загрузка MOEX
└── pipelines.py              # сборка пайплайнов для DAG-ов

tests/                        # pytest на чистые функции и сборку пайплайнов
analytics/                    # dbt-проект
infra/initdb/                 # bootstrap-скрипты схемы БД
infra/migrations/             # миграции для уже существующих БД
```

DAG-файлы остаются тонкими: вся логика — в пакете `etl`, который монтируется в контейнеры
(`/opt/python`) и доступен тестам без Airflow.

## DAG-и Airflow

| DAG ID | Расписание | Описание |
|--------|------------|----------|
| `daily_moex_data_loader` | `0 8 * * *` (MSK) | `load_moex_prices`: окно по `logical_date` → `stock_market.prices_moex` |
| `daily_nyse_data_loader` | `0 8 * * *` (MSK) | `load_yfinance_prices`: окно по `logical_date` → `stock_market.prices_yfinance` |
| `run_dbt_after_sources` | `0 9 * * *` (MSK) | Ожидание обоих загрузчиков → `dbt run` → `dbt test` |

У загрузчиков включены `catchup=True` (догоняем пропущенные дни), `max_active_runs=1`
(нет параллельных запусков) и `retries=2` с задержкой 5 минут.

## Требования

- Docker и Docker Compose
- Рекомендуется ≥ 4 GB RAM и 2 CPU для Airflow (см. предупреждения в `docker-compose`)
- Токен API Московской биржи (Algopack) для MOEX
- Внешняя PostgreSQL не нужна: база с рыночными данными поднимается в контейнере `market-postgres`

## Быстрый старт

### 1. Переменные окружения

Скопируйте шаблон и заполните значения:

```bash
cp .env.example .env
```

| Переменная | Назначение |
|------------|------------|
| `DATABASE_URL` | Подключение SQLAlchemy к БД с рыночными данными (используют загрузчики Airflow) |
| `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASS`, `DB_NAME` | Параметры PostgreSQL для dbt (`analytics/profiles.yml`) и для самого контейнера `market-postgres` |
| `DB_HOST_PORT` | Порт, на который `market-postgres` публикуется на хост (по умолчанию `5433`) |
| `MOEX_API_TOKEN` | Токен API Algopack MOEX |
| `LOADER_LOOKBACK_DAYS` | На сколько дней назад расширять окно загрузки (по умолчанию `3`) |

Переменные из `.env` прокидываются в `docker-compose.yaml` и в профиль dbt.

Профиль dbt лежит в репозитории (`analytics/profiles.yml`) и не содержит секретов: все параметры подключения берутся из `DB_*` через `env_var()`.

> Файл `.env` в `.gitignore` — не коммитьте секреты.

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

> Сервис `dbt` собирается из того же образа, что и Airflow. В `docker compose up` он выполняет только `dbt --version` и завершается — он нужен для ручных запусков выше.

## База с рыночными данными

Сервис `market-postgres` создаёт схему `stock_market` и таблицы `prices_moex`, `prices_yfinance`, `instruments` скриптами из `infra/initdb` при **первой** инициализации тома.

```bash
# посмотреть, что создалось
docker compose exec market-postgres psql -U market -d market -c '\dt stock_market.*'

# применить bootstrap-скрипты к уже существующему тому (они не выполняются повторно автоматически)
docker compose exec -T market-postgres psql -U market -d market -f - < infra/initdb/01_stock_market.sql

# миграции для существующих БД (уникальные индексы, нужны для идемпотентной записи)
docker compose exec -T market-postgres psql -U market -d market -f - < infra/migrations/001_unique_prices.sql
```

Подключение из IDE/psql с хоста: `localhost:5433`, база/пользователь/пароль — `market` (см. `.env`).
Для Metabase внутри compose: хост `market-postgres`, порт `5432`, база `market`.

> `docker compose down -v` удаляет тома `postgres-db-volume` и `market-db-volume`, то есть рыночные данные тоже.

## Инкрементальная загрузка

Каждый запуск загрузчика привязан к `logical_date` (точнее — к `data_interval`) и грузит окно
`[data_interval_start − LOADER_LOOKBACK_DAYS, data_interval_end)`:

- `data_interval_end` — эксклюзивная граница, поэтому незакрытая свеча текущего дня в БД не попадает;
- `LOADER_LOOKBACK_DAYS` (по умолчанию 3 дня) — страховка от того, что источник отдаёт последние свечи с задержкой;
- пустая таблица → bootstrap: грузится история с `HISTORY_START_DATE` (2020-01-01);
- запись идемпотентна: `INSERT ... ON CONFLICT (ticker, price_date) DO NOTHING` — повторный запуск за ту же дату вставляет 0 строк;
- `catchup=True` + `max_active_runs=1`: пропущенные интервалы догоняются, параллельных запусков нет.

Backfill за прошлый период (Airflow 3: команда `backfill create`, а не `dags backfill`):

```bash
docker compose run --rm airflow-cli airflow backfill create \
  --dag-id daily_nyse_data_loader \
  --from-date 2026-11-01 --to-date 2026-11-03
```

> CLI Airflow запускайте через сервис `airflow-cli` (`docker compose run --rm airflow-cli airflow ...`).
> `docker compose exec <service> airflow ...` под root не работает: пакеты Airflow лежат в user-site,
> а `PYTHONPATH` выставляет только `/entrypoint`.

> При первом включении DAG-а Airflow поставит в очередь все интервалы с `start_date` (2025-10-22).
> Это безопасно (upsert), но занимает время; если история не нужна — сдвиньте `start_date`.


## dbt

Проект `analytics` объявляет источники в `models/sources.yaml`:

- `stock_market.prices_moex`
- `stock_market.prices_yfinance`
- `stock_market.instruments`

Слои моделей: `staging` (view) → `marts` (`fct_all_prices` приводит цены к рублям по курсу `RUB=X`).
Профиль подключения — `analytics/profiles.yml` (параметры из переменных окружения `DB_*`).

## Тесты

Логика загрузчиков покрыта тестами на чистых функциях (без БД и внешних API):

```bash
pip install -r requirements-dev.txt
python -m pytest
```

## Зависимости Python (образ Airflow)

Основные пакеты из `requirements.txt`: `pandas`, `sqlalchemy`, `psycopg2-binary`, `moexalgo`, `yfinance`, `python-dotenv`, `dbt-core` + `dbt-postgres`, провайдер Airflow для PostgreSQL. DAG оркестрации запускает dbt прямо в контейнере Airflow, поэтому `docker.sock` в контейнеры не пробрасывается.
