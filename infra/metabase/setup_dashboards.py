"""Разворачивает дашборд в Metabase поверх витрин dbt.

Идемпотентно: повторный запуск обновляет карточки, фильтры и дашборд, а не создаёт дубли.

Использование (Metabase поднят в docker-compose):
    python3 infra/metabase/setup_dashboards.py

Переменные окружения:
    METABASE_URL        адрес Metabase (по умолчанию http://localhost:3000)
    MB_ADMIN_EMAIL      e-mail администратора (по умолчанию admin@example.com)
    MB_ADMIN_PASSWORD   пароль администратора (по умолчанию Metabase!2026)
    MB_SITE_NAME        название инстанса (по умолчанию Finance Project)
    DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASS — подключение к market-postgres
"""

from __future__ import annotations

import hashlib
import json
import os
import pathlib
import sys
import time
import urllib.error
import urllib.request

BASE_URL = os.getenv("METABASE_URL", "http://localhost:3000").rstrip("/")
ADMIN_EMAIL = os.getenv("MB_ADMIN_EMAIL", "admin@example.com")
ADMIN_PASSWORD = os.getenv("MB_ADMIN_PASSWORD", "Metabase!2026")
SITE_NAME = os.getenv("MB_SITE_NAME", "Finance Project")

DB_NAME = "market"
DB_DETAILS = {
    "host": os.getenv("DB_HOST", "market-postgres"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "market"),
    "user": os.getenv("DB_USER", "market"),
    "password": os.getenv("DB_PASS", "market"),
    "ssl": False,
}

DASHBOARD_NAME = "Финансовые рынки"
DASHBOARD_DESCRIPTION = "Обзор по акциям, ETF, крипте, валютам, индексам и сырью (витрины dbt)"
CARDS_DIR = pathlib.Path(__file__).parent / "cards"

# Фильтры дашборда: имя → (подпись, тип параметра, секция в UI)
FILTERS = {
    "period": ("Период", "date/all-options", "date"),
    "asset_class": ("Класс актива", "category", "string"),
    "ticker": ("Тикер", "category", "string"),
}

# Карточки: файл с SQL, заголовок, тип визуализации, настройки,
# таблица-источник (для поиска field id) и фильтры (имя фильтра → колонка)
CARDS = [
    {
        "file": "market_overview_daily.sql",
        "title": "Средняя доходность по классам активов",
        "display": "line",
        "settings": {"graph.dimensions": ["price_date", "instrument_type"], "graph.metrics": ["avg_daily_return"]},
        "table": "fct_market_overview",
        "filters": {"period": "price_date", "asset_class": "instrument_type"},
    },
    {
        "file": "breadth_daily.sql",
        "title": "Ширина рынка: растущие vs падающие",
        "display": "bar",
        "settings": {"graph.dimensions": ["price_date"], "graph.metrics": ["advancers", "decliners"]},
        "table": "fct_market_overview",
        "filters": {"period": "price_date"},
    },
    {
        "file": "prices_normalized.sql",
        "title": "Динамика цен (нормировано к 100)",
        "display": "line",
        "settings": {"graph.dimensions": ["price_date", "ticker"], "graph.metrics": ["index_100"]},
        "table": "fct_prices_daily",
        "filters": {"period": "price_date", "asset_class": "instrument_type", "ticker": "ticker"},
    },
    {
        "file": "stock_vs_index.sql",
        "title": "Акция vs индекс S&P 500",
        "display": "line",
        "settings": {"graph.dimensions": ["price_date", "ticker"], "graph.metrics": ["index_100"]},
        "table": "fct_prices_daily",
        "filters": {"period": "price_date", "ticker": "ticker"},
    },
    {
        "file": "close_rub_table.sql",
        "title": "Цены в рублях: последняя дата по инструменту",
        "display": "table",
        "settings": {},
        "table": "fct_prices_daily",
        "filters": {"asset_class": "instrument_type", "ticker": "ticker"},
    },
    {
        "file": "usd_rub_daily.sql",
        "title": "Курс USD/RUB",
        "display": "line",
        "settings": {"graph.dimensions": ["price_date"], "graph.metrics": ["usd_rub"]},
        "table": "int_fx_daily",
        "filters": {"period": "price_date"},
    },
    {
        "file": "volatility_top.sql",
        "title": "Волатильность 30 дней",
        "display": "line",
        "settings": {"graph.dimensions": ["price_date", "ticker"], "graph.metrics": ["volatility_30d"]},
        "table": "fct_prices_daily",
        "filters": {"period": "price_date", "asset_class": "instrument_type", "ticker": "ticker"},
    },
    {
        "file": "volume_leaders.sql",
        "title": "Лидеры по объёму торгов",
        "display": "table",
        "settings": {},
        "table": "fct_volume_leaders",
        "filters": {"period": "price_date", "asset_class": "instrument_type", "ticker": "ticker"},
    },
    {
        "file": "dividends_last.sql",
        "title": "Последние дивиденды и доходность",
        "display": "table",
        "settings": {},
        "table": "fct_dividends",
        "filters": {"period": "ex_date", "asset_class": "instrument_type"},
    },
    {
        "file": "splits_last.sql",
        "title": "Сплиты акций",
        "display": "table",
        "settings": {},
        "table": "fct_splits",
        "filters": {"period": "split_date", "asset_class": "instrument_type", "ticker": "ticker"},
    },
]


class MetabaseError(RuntimeError):
    pass


def tag_id(filename: str, filter_name: str) -> str:
    """Детерминированный id тега (Metabase использует его для матчинга параметров)."""
    return hashlib.md5(f"{filename}:{filter_name}".encode()).hexdigest()[:8]


def param_id(filter_name: str) -> str:
    """Детерминированный id параметра дашборда."""
    return hashlib.md5(f"dashboard:{filter_name}".encode()).hexdigest()[:8]


def request(method: str, path: str, payload: dict | None = None, session: str | None = None):
    """HTTP-запрос к API Metabase на стандартной библиотеке."""
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {"Content-Type": "application/json"}
    if session:
        headers["X-Metabase-Session"] = session

    req = urllib.request.Request(f"{BASE_URL}{path}", data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=180) as response:
            body = response.read().decode("utf-8")
            return json.loads(body) if body else None
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise MetabaseError(f"{method} {path} -> {exc.code}: {detail[:400]}") from exc


def rows_of(response) -> list[dict]:
    """Достаёт список объектов из ответа (часть ручек Metabase пагинирована)."""
    if isinstance(response, list):
        return response
    if isinstance(response, dict):
        return response.get("data") or []
    return []


def authenticate() -> str:
    """Первичная настройка инстанса или логин существующим администратором."""
    properties = request("GET", "/api/session/properties") or {}
    setup_token = properties.get("setup-token")

    if setup_token:
        print(f"Metabase не настроен — создаю администратора {ADMIN_EMAIL}")
        request(
            "POST",
            "/api/setup",
            {
                "token": setup_token,
                "user": {"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD, "first_name": "Airflow"},
                "prefs": {"site_name": SITE_NAME},
            },
        )

    session = request("POST", "/api/session", {"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD})
    return session["id"]


def ensure_database(session: str) -> int:
    """Возвращает id подключения к market-postgres, создавая его при необходимости."""
    existing = [db for db in rows_of(request("GET", "/api/database", session=session)) if db.get("name") == DB_NAME]
    if existing:
        print(f"Подключение «{DB_NAME}» уже есть (id={existing[0]['id']})")
        return existing[0]["id"]

    created = request(
        "POST",
        "/api/database",
        {"engine": "postgres", "name": DB_NAME, "details": DB_DETAILS, "is_full_sync": True},
        session=session,
    )
    print(f"Создано подключение «{DB_NAME}» (id={created['id']})")
    return created["id"]


def resolve_fields(session: str, database_id: int, tables: set[str]) -> dict[tuple[str, str], tuple[int, str]]:
    """Синхронизирует схему и возвращает id/типы полей: (таблица, колонка) → (field_id, base_type)."""
    request("POST", f"/api/database/{database_id}/sync_schema", session=session)

    for _ in range(36):
        meta = request("GET", f"/api/database/{database_id}/metadata", session=session) or {}
        found = {table["name"]: table for table in meta.get("tables", [])}
        if tables <= set(found):
            fields: dict[tuple[str, str], tuple[int, str]] = {}
            for table_name in tables:
                for field in found[table_name].get("fields", []):
                    fields[(table_name, field["name"])] = (field["id"], field.get("base_type"))
            return fields
        time.sleep(5)

    missing = sorted(tables - set(found))
    raise MetabaseError(f"Metabase не синхронизировал таблицы: {missing}")


def build_tags(spec: dict, fields: dict[tuple[str, str], tuple[int, str]]) -> dict:
    """Собирает template-tags (field-фильтры) для карточки."""
    tags = {}
    for filter_name, column in spec["filters"].items():
        field_id, base_type = fields[(spec["table"], column)]
        tags[filter_name] = {
            "id": tag_id(spec["file"], filter_name),
            "name": filter_name,
            "display-name": FILTERS[filter_name][0],
            "type": "dimension",
            "dimension": ["field", field_id, {"base-type": base_type}],
            "widget-type": FILTERS[filter_name][1],
        }
    return tags


def ensure_card(
    session: str,
    database_id: int,
    spec: dict,
    fields: dict[tuple[str, str], tuple[int, str]],
) -> tuple[int, dict]:
    """Создаёт или обновляет карточку (native SQL-вопрос) и возвращает (card_id, tags)."""
    sql_path = CARDS_DIR / spec["file"]
    if not sql_path.exists():
        raise MetabaseError(f"нет файла с SQL: {sql_path}")

    tags = build_tags(spec, fields)
    payload = {
        "name": spec["title"],
        "description": f"Источник: {spec['file']}",
        "display": spec["display"],
        "dataset_query": {
            "database": database_id,
            "type": "native",
            "native": {"query": sql_path.read_text(encoding="utf-8"), "template-tags": tags},
        },
        "visualization_settings": spec["settings"],
    }

    existing = [c for c in rows_of(request("GET", "/api/card", session=session)) if c.get("name") == spec["title"]]
    if existing:
        card_id = existing[0]["id"]
        request("PUT", f"/api/card/{card_id}", payload, session=session)
        print(f"  обновлена карточка «{spec['title']}» (id={card_id})")
    else:
        created = request("POST", "/api/card", payload, session=session)
        card_id = created["id"]
        print(f"  создана карточка «{spec['title']}» (id={card_id})")

    result = request("POST", f"/api/card/{card_id}/query", session=session) or {}
    row_count = len((result.get("data") or {}).get("rows") or [])
    print(f"    проверка запроса: status={result.get('status')}, строк={row_count}")

    return card_id, tags



def ensure_dashboard(session: str, cards: list[tuple[dict, int, dict]]) -> int:
    """Создаёт/обновляет дашборд: параметры (фильтры) и раскладку карточек."""
    payload = {"name": DASHBOARD_NAME, "description": DASHBOARD_DESCRIPTION}
    existing = [d for d in rows_of(request("GET", "/api/dashboard", session=session)) if d.get("name") == DASHBOARD_NAME]

    if existing:
        dashboard_id = existing[0]["id"]
        request("PUT", f"/api/dashboard/{dashboard_id}", payload, session=session)
        print(f"Дашборд «{DASHBOARD_NAME}» уже есть (id={dashboard_id})")
        current = request("GET", f"/api/dashboard/{dashboard_id}", session=session) or {}
        saved = {card.get("card_id"): card.get("id") for card in current.get("dashcards", [])}
    else:
        created = request("POST", "/api/dashboard", payload, session=session)
        dashboard_id = created["id"]
        saved = {}
        print(f"Создан дашборд «{DASHBOARD_NAME}» (id={dashboard_id})")

    # Параметры дашборда — только те фильтры, которые используются карточками
    used_filters = {name for spec, _, _ in cards for name in spec["filters"]}
    parameters = [
        {
            "id": param_id(name),
            "name": FILTERS[name][0],
            "slug": name.replace("_", "-"),
            "type": FILTERS[name][1],
            "sectionId": FILTERS[name][2],
        }
        for name in FILTERS
        if name in used_filters
    ]

    dashcards = []
    for index, (spec, card_id, tags) in enumerate(cards):
        mappings = [
            {
                "parameter_id": param_id(filter_name),
                "card_id": card_id,
                "target": ["dimension", ["template-tag", filter_name]],
            }
            for filter_name in spec["filters"]
        ]
        dashcards.append(
            {
                # отрицательный id — признак новой карточки для Metabase
                "id": saved.get(card_id, -(index + 1)),
                "card_id": card_id,
                "row": (index // 2) * 6,
                "col": (index % 2) * 12,
                "size_x": 12,
                "size_y": 6,
                "parameter_mappings": mappings,
                "visualization_settings": {},
            }
        )

    request(
        "PUT",
        f"/api/dashboard/{dashboard_id}",
        {"parameters": parameters, "dashcards": dashcards},
        session=session,
    )
    print(f"  фильтры: {', '.join(parameter['name'] for parameter in parameters)}")
    return dashboard_id


def archive_stale_cards(session: str, active_titles: set[str]) -> None:
    """Архивирует карточки, созданные этим скриптом, но больше не описанные в CARDS."""
    stale = [
        card
        for card in rows_of(request("GET", "/api/card", session=session))
        if not card.get("archived")
        and str(card.get("description") or "").startswith("Источник: ")
        and card.get("name") not in active_titles
    ]
    for card in stale:
        request("PUT", f"/api/card/{card['id']}", {"archived": True}, session=session)
        print(f"  архивирована устаревшая карточка «{card['name']}» (id={card['id']})")


def main() -> int:
    session = authenticate()
    database_id = ensure_database(session)

    tables = {spec["table"] for spec in CARDS}
    fields = resolve_fields(session, database_id, tables)
    print(f"Синхронизировано полей: {len(fields)}")

    cards = []
    for spec in CARDS:
        card_id, tags = ensure_card(session, database_id, spec, fields)
        cards.append((spec, card_id, tags))

    archive_stale_cards(session, {spec["title"] for spec in CARDS})

    dashboard_id = ensure_dashboard(session, cards)

    print(f"\nГотово: {BASE_URL}/dashboard/{dashboard_id}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except MetabaseError as error:
        print(f"Ошибка Metabase: {error}", file=sys.stderr)
        sys.exit(1)

