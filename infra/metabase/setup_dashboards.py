"""Разворачивает дашборд в Metabase поверх витрин dbt.

Идемпотентно: повторный запуск обновляет карточки и дашборд, а не создаёт дубли.

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

import json
import os
import pathlib
import sys
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

# (файл с SQL, название карточки, тип визуализации, настройки визуализации)
CARDS = [
    (
        "market_overview_daily.sql",
        "Средняя доходность по классам активов",
        "line",
        {"graph.dimensions": ["price_date", "instrument_type"], "graph.metrics": ["avg_daily_return"]},
    ),
    (
        "breadth_daily.sql",
        "Ширина рынка: растущие vs падающие",
        "bar",
        {"graph.dimensions": ["price_date"], "graph.metrics": ["advancers", "decliners"]},
    ),
    (
        "prices_normalized.sql",
        "Динамика цен (нормировано к 100)",
        "line",
        {"graph.dimensions": ["price_date", "ticker"], "graph.metrics": ["index_100"]},
    ),
    (
        "close_rub_table.sql",
        "Цены в рублях: последний торговый день",
        "table",
        {},
    ),
    (
        "usd_rub_daily.sql",
        "Курс USD/RUB",
        "line",
        {"graph.dimensions": ["price_date"], "graph.metrics": ["usd_rub"]},
    ),
    (
        "volatility_top.sql",
        "Волатильность 30 дней",
        "line",
        {"graph.dimensions": ["price_date", "ticker"], "graph.metrics": ["volatility_30d"]},
    ),
    (
        "volume_leaders.sql",
        "Лидеры по объёму торгов",
        "table",
        {},
    ),
    (
        "dividends_last.sql",
        "Последние дивиденды и доходность",
        "table",
        {},
    ),
]


class MetabaseError(RuntimeError):
    pass


def request(method: str, path: str, payload: dict | None = None, session: str | None = None):
    """HTTP-запрос к API Metabase на стандартной библиотеке."""
    url = f"{BASE_URL}{path}"
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {"Content-Type": "application/json"}
    if session:
        headers["X-Metabase-Session"] = session

    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=60) as response:
            body = response.read().decode("utf-8")
            return json.loads(body) if body else None
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise MetabaseError(f"{method} {path} -> {exc.code}: {detail[:400]}") from exc


def rows(response) -> list[dict]:
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
    existing = [db for db in rows(request("GET", "/api/database", session=session)) if db.get("name") == DB_NAME]
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


def ensure_card(
    session: str,
    database_id: int,
    sql_path: pathlib.Path,
    title: str,
    display: str,
    settings: dict,
) -> int:
    """Создаёт или обновляет карточку (native SQL-вопрос) и возвращает её id."""
    payload = {
        "name": title,
        "description": f"Источник: {sql_path.name}",
        "display": display,
        "dataset_query": {
            "database": database_id,
            "type": "native",
            "native": {"query": sql_path.read_text(encoding="utf-8"), "template-tags": {}},
        },
        "visualization_settings": settings,
    }

    existing = [card for card in rows(request("GET", "/api/card", session=session)) if card.get("name") == title]
    if existing:
        card_id = existing[0]["id"]
        request("PUT", f"/api/card/{card_id}", payload, session=session)
        print(f"  обновлена карточка «{title}» (id={card_id})")
        return card_id

    created = request("POST", "/api/card", payload, session=session)
    print(f"  создана карточка «{title}» (id={created['id']})")
    return created["id"]


def ensure_dashboard(session: str, card_ids: list[int]) -> int:
    """Создаёт или обновляет дашборд и раскладывает карточки по сетке."""
    payload = {"name": DASHBOARD_NAME, "description": DASHBOARD_DESCRIPTION}
    existing = [d for d in rows(request("GET", "/api/dashboard", session=session)) if d.get("name") == DASHBOARD_NAME]

    if existing:
        dashboard_id = existing[0]["id"]
        request("PUT", f"/api/dashboard/{dashboard_id}", payload, session=session)
        print(f"Дашборд «{DASHBOARD_NAME}» уже есть (id={dashboard_id})")
        current = request("GET", f"/api/dashboard/{dashboard_id}", session=session) or {}
        saved = {dc.get("card_id"): dc.get("id") for dc in current.get("dashcards", [])}
    else:
        created = request("POST", "/api/dashboard", payload, session=session)
        dashboard_id = created["id"]
        saved = {}
        print(f"Создан дашборд «{DASHBOARD_NAME}» (id={dashboard_id})")

    dashcards = [
        {
            # отрицательный id — признак новой карточки для Metabase
            "id": saved.get(card_id, -(index + 1)),
            "card_id": card_id,
            "row": (index // 2) * 6,
            "col": (index % 2) * 12,
            "size_x": 12,
            "size_y": 6,
            "parameter_mappings": [],
            "visualization_settings": {},
        }
        for index, card_id in enumerate(card_ids)
    ]

    request("PUT", f"/api/dashboard/{dashboard_id}", {"dashcards": dashcards}, session=session)
    return dashboard_id


def main() -> int:
    session = authenticate()
    database_id = ensure_database(session)

    card_ids = []
    for filename, title, display, settings in CARDS:
        sql_path = CARDS_DIR / filename
        if not sql_path.exists():
            raise MetabaseError(f"нет файла с SQL: {sql_path}")
        card_ids.append(ensure_card(session, database_id, sql_path, title, display, settings))

    dashboard_id = ensure_dashboard(session, card_ids)

    print(f"\nГотово: {BASE_URL}/dashboard/{dashboard_id}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except MetabaseError as error:
        print(f"Ошибка Metabase: {error}", file=sys.stderr)
        sys.exit(1)

