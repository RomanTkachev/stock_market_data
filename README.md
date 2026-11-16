# Finance Project

Проект для сбора, хранения и аналитической обработки рыночных данных. Пайплайн автоматизирован через **Apache Airflow**, сырые данные лежат в **PostgreSQL**, трансформации планируются в **dbt**, дашборды — в **Metabase**.

## Возможности

- **Ежедневная загрузка рыночных данных Yahoo Finance**: акции, ETF, криптовалюты, валюты, индексы и сырьё (19 инструментов) → `stock_market.prices` (OHLCV + Adj Close).
- **Корпоративные действия и справочник**: дивиденды, сплиты и метаданные инструментов (класс актива, биржа, отрасль, капитализация) → `dividends`, `splits`, `instruments`.
- **Инкрементальная загрузка по logical_date**: каждый запуск грузит окно `[data_interval_start − LOADER_LOOKBACK_DAYS, data_interval_end)`, запись идемпотентна (`INSERT ... ON CONFLICT DO NOTHING`), поэтому повторные и параллельные запуски не создают дублей, а пропущенные дни догоняются через `catchup`.
- **Многослойный dbt**: `staging → intermediate → marts` — цены в рублях, доходности, волатильность, скользящие средние, дивидендная доходность, рыночная картина дня; 25 dbt-тестов.
- **Оркестрация dbt**: отдельный DAG ждёт успешного завершения загрузчика цен и запускает `dbt run` и `dbt test` (dbt установлен в образ Airflow).
- **Локальный стек в Docker**: Airflow (CeleryExecutor), PostgreSQL с метаданными Airflow, PostgreSQL с рыночными данными (`market-postgres`), Redis, Metabase.

## Архитектура

```
                     ┌───────────────────┐
                     │  Yahoo Finance    │
                     │  (yfinance)       │
                     └─────────┬─────────┘
                               ▼
 ┌─────────────────────────────────────────────────────┐
 │                Apache Airflow (DAGs)                │
 │  daily_prices_loader (08:00 MSK)                    │
 │  weekly_reference_loader (пн 07:00 MSK)             │
 └──────────────────────────┬──────────────────────────┘
                            ▼
         ┌──────────────────────────────────┐
         │  PostgreSQL market-postgres      │
         │  schema: stock_market            │
         │  prices · dividends · splits     │
         │  instruments                     │
         └───────────────┬──────────────────┘
                         ▼
         ┌────────────────────────────────────────┐
         │  dbt (в образе Airflow)                │
         │  staging → intermediate → marts        │
         │  run_dbt_after_sources (09:00 MSK)     │
         └───────────────┬────────────────────────┘
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
| Загрузка данных | [yfinance](https://github.com/ranaroussi/yfinance) |
| Трансформации | dbt-core + dbt-postgres 1.9.x (внутри образа Airflow) |
| Визуализация | Metabase |
| БД | PostgreSQL 16 (метаданные Airflow + `market-postgres` с рыночными данными) |
| Контейнеризация | Docker Compose |


## Структура репозитория

```text
airflow_setup/
├── dags/
│   ├── prices_dag/dag.py      # DAG загрузки цен (ежедневно)
│   ├── reference_dag/dag.py   # дивиденды, сплиты, метаданные (еженедельно)
│   └── dbt_dag/dag.py         # DAG-оркестратор dbt
├── config/airflow.cfg
└── logs/

etl/                          # ETL-логика: импортируется DAG-ами и тестами
├── config.py                 # инвестиционная вселенная, lookback, начало истории
├── dates.py                  # окно загрузки, заполнение пропусков, дедупликация
├── db.py                     # engine, последние даты, идемпотентный upsert
├── yfinance_source.py        # цены OHLCV, дивиденды/сплиты, метаданные
└── pipelines.py              # сборка пайплайнов для DAG-ов

tests/                        # pytest на чистые функции и сборку пайплайнов
analytics/                    # dbt-проект (staging → intermediate → marts)
infra/initdb/                 # bootstrap-скрипты схемы БД
infra/migrations/             # миграции для уже существующих БД
infra/metabase/               # дашборд как код: SQL-карточки + скрипт провижининга
```

DAG-файлы остаются тонкими: вся логика — в пакете `etl`, который монтируется в контейнеры
(`/opt/python`) и доступен тестам без Airflow.

## DAG-и Airflow

| DAG ID | Расписание | Описание |
|--------|------------|----------|
| `daily_prices_loader` | `0 8 * * *` (MSK) | `load_prices`: окно по `logical_date` → `stock_market.prices` |
| `weekly_reference_loader` | `0 7 * * 1` (MSK) | `load_actions` (дивиденды, сплиты) → `update_instruments` (справочник) |
| `run_dbt_after_sources` | `0 9 * * *` (MSK) | Ждёт `daily_prices_loader` → `dbt run` → `dbt test` |

У загрузчика цен включены `catchup=True` (догоняем пропущенные дни), `max_active_runs=1`
(нет параллельных запусков) и `retries=2` с задержкой 5 минут.
Example-DAG-и Airflow отключены (`AIRFLOW__CORE__LOAD_EXAMPLES: 'false'`).

## Требования

- Docker и Docker Compose
- Рекомендуется ≥ 4 GB RAM и 2 CPU для Airflow (см. предупреждения в `docker-compose`)
- Внешние API-ключи не нужны: все данные берутся из публичного Yahoo Finance
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
- **Metabase**: http://localhost:3000 — администратор `admin@example.com` / `Metabase!2026`
  (создаётся скриптом `infra/metabase/setup_dashboards.py`, см. раздел «Дашборды в Metabase»)
- **dbt docs** (lineage): http://localhost:8081 — поднимается сервисом `dbt-docs`

### 3. Включение DAG-ов

В UI Airflow включите (unpause) DAG-и загрузки и оркестрации dbt. По умолчанию DAG-и создаются на паузе (`DAGS_ARE_PAUSED_AT_CREATION`).

### 4. Ручной запуск dbt (опционально)

```bash
docker compose run --rm dbt run
docker compose run --rm dbt test
```

> Сервис `dbt` собирается из того же образа, что и Airflow. В `docker compose up` он выполняет только `dbt --version` и завершается — он нужен для ручных запусков выше.

## База с рыночными данными

Сервис `market-postgres` создаёт схему `stock_market` и таблицы `prices`, `dividends`, `splits`, `instruments` скриптами из `infra/initdb` при **первой** инициализации тома.

```bash
# посмотреть, что создалось
docker compose exec market-postgres psql -U market -d market -c '\dt stock_market.*'

# применить bootstrap-скрипты к уже существующему тому (они не выполняются повторно автоматически)
docker compose exec -T market-postgres psql -U market -d market -f - < infra/initdb/01_stock_market.sql

# миграции для существующих БД (уникальные индексы для цен)
docker compose exec -T market-postgres psql -U market -d market -f - < infra/migrations/001_unique_prices.sql

# переход на один источник (Yahoo Finance) и OHLCV: prices_yfinance → prices, дивиденды/сплиты
docker compose exec -T market-postgres psql -U market -d market -f - < infra/migrations/002_yfinance_only.sql
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
  --dag-id daily_prices_loader \
  --from-date 2026-11-01 --to-date 2026-11-03
```

> CLI Airflow запускайте через сервис `airflow-cli` (`docker compose run --rm airflow-cli airflow ...`).
> `docker compose exec <service> airflow ...` под root не работает: пакеты Airflow лежат в user-site,
> а `PYTHONPATH` выставляет только `/entrypoint`.

> При первом включении DAG-а Airflow поставит в очередь все интервалы с `start_date` (2025-10-22).
> Это безопасно (upsert), но занимает время; если история не нужна — сдвиньте `start_date`.


## dbt

Проект `analytics` объявляет источники в `models/sources.yaml`: `stock_market.prices`,
`instruments`, `dividends`, `splits`.

Слои моделей:

| Слой | Модели | Что делает |
|------|--------|------------|
| staging (view) | `stg_prices`, `stg_instruments`, `stg_dividends`, `stg_splits` | типизация, чистка, `coalesce(volume, 0)` |
| intermediate (view) | `int_fx_daily`, `int_prices_enriched` | курсы USD/RUB и EUR/USD, обогащение атрибутами, пересчёт в рубли |
| marts (table) | `fct_prices_daily` | OHLCV, `close_rub`, `daily_return`, `log_return`, `gap_pct`, `range_pct`, `ma_7/30`, `volatility_30d` |
| | `fct_dividends` | дивиденды + доходность на дату отсечки |
| | `fct_market_overview` | по классам активов: средняя доходность, advancers/decliners, breadth, объём |
| | `fct_volume_leaders` | лидеры по объёму за день (rank) |
| | `dim_instruments` | справочник для срезов |

Тесты: `schema.yml` в `staging/` и `marts/` (`unique`, `not_null`, `accepted_values`,
`relationships`) + сингулярные тесты `tests/assert_prices_unique_combination.sql`
и `tests/assert_prices_valid_ohlc.sql` — всего 25 проверок.

Профиль подключения — `analytics/profiles.yml` (параметры из переменных окружения `DB_*`).

## Дашборды в Metabase

Дашборд «Финансовые рынки» разворачивается кодом — карточки описаны SQL-файлами в
`infra/metabase/cards/`, а скрипт `infra/metabase/setup_dashboards.py` создаёт подключение к БД,
карточки и дашборд через API Metabase:

```bash
python3 infra/metabase/setup_dashboards.py
```

Скрипт идемпотентен: повторный запуск обновляет карточки, фильтры и раскладку дашборда, дублей не создаёт,
а карточки, удалённые из репозитория, автоматически уходят в архив. Меняется только SQL в
`infra/metabase/cards/*.sql` — после правки достаточно запустить скрипт ещё раз.

**Фильтры дашборда** (field-фильтры Metabase, id полей скрипт находит сам через синхронизацию схемы):

| Фильтр | Тип | Что делает |
|--------|-----|------------|
| `Период` | дата (все варианты) | ограничивает диапазон дат в карточках |
| `Класс актива` | категория | `stock` / `etf` / `crypto` / `fx` / `index` / `commodity` |
| `Тикер` | категория | конкретный инструмент (например `AAPL`) |

| Переменная | Назначение |
|------------|------------|
| `METABASE_URL` | адрес Metabase (по умолчанию `http://localhost:3000`) |
| `MB_ADMIN_EMAIL` / `MB_ADMIN_PASSWORD` | администратор Metabase (по умолчанию `admin@example.com` / `Metabase!2026`) |
| `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASS` | подключение к `market-postgres` (Metabase ходит к БД изнутри compose-сети) |

> При первом запуске скрипт сам выполняет первичную настройку Metabase (создаёт администратора).
> Если инстанс уже настроен другим администратором — задайте `MB_ADMIN_EMAIL` / `MB_ADMIN_PASSWORD`.

Карточки дашборда (10): средняя доходность по классам активов, ширина рынка (advancers/decliners),
динамика цен (нормировано к 100), **акция vs индекс S&P 500**, цены в рублях по каждому инструменту,
курс USD/RUB, волатильность 30 дней, лидеры по объёму, последние дивиденды с доходностью, **сплиты акций**.

Зависимость дашборда от моделей dbt зафиксирована как exposure
`analytics/models/exposures.yml` — она видна в lineage `dbt docs`.

## Документация dbt

Документация с lineage и каталогом колонок поднимается отдельным сервисом:

```bash
docker compose up -d dbt-docs
```

Открыть http://localhost:8081 — сервис сам выполняет `dbt docs generate` (строит `manifest.json` +
`catalog.json`) и раздаёт статический сайт. В графе видно путь `sources → staging → intermediate → marts`
и exposure дашборда Metabase.

## Тесты

Логика загрузчиков покрыта тестами на чистых функциях (без БД и внешних API):

```bash
pip install -r requirements-dev.txt
python -m pytest
```

## Зависимости Python (образ Airflow)

Основные пакеты из `requirements.txt`: `pandas`, `sqlalchemy`, `psycopg2-binary`, `yfinance`, `python-dotenv`, `dbt-core` + `dbt-postgres`, провайдер Airflow для PostgreSQL. DAG оркестрации запускает dbt прямо в контейнере Airflow, поэтому `docker.sock` в контейнеры не пробрасывается.
