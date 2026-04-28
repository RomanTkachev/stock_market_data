# dags/scripts/yfinance_loader.py

import os
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
import logging
from datetime import date, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.types import Date, VARCHAR, Numeric

def fill_missing_dates(df, date_col='price_date', ticker_col='ticker', price_col='price'):
    """
    Заполняет пропущенные календарные дни для каждого тикера.
    """
    logging.info("Начинаем заполнение пропущенных дат...")
    
    df[date_col] = pd.to_datetime(df[date_col])
    
    all_filled_dfs = []
    
    for ticker_name, group_df in df.groupby(ticker_col):
        ticker_df = group_df.set_index(date_col).sort_index()
        min_date, max_date = ticker_df.index.min(), ticker_df.index.max()
        
        # Проверяем, что даты валидны
        if pd.isna(min_date) or pd.isna(max_date):
            logging.warning("Невозможно определить диапазон дат для тикера %s, пропускаем.", ticker_name)
            continue
            
        full_range = pd.date_range(start=min_date, end=max_date, freq='D')
        
        resampled_df = ticker_df.reindex(full_range).ffill().bfill()
        resampled_df[ticker_col] = ticker_name
        all_filled_dfs.append(resampled_df)
        
    if not all_filled_dfs:
        return pd.DataFrame()

    final_df = pd.concat(all_filled_dfs).reset_index().rename(columns={'index': date_col})
    logging.info("Заполнение пропущенных дат завершено.")
    return final_df

def load_yfinance_data_to_db():
    """
    Загружает дневные котировки yfinance, заполняет пропуски, 
    оставляет новые данные и загружает их в 'prices_yfinance'.
    """
    load_dotenv()
    
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        logging.error("Не удалось загрузить DATABASE_URL.")
        raise ValueError("DATABASE_URL не настроен.")
    engine = create_engine(db_url)
    


    tickers_list = ['AAPL', 'MSFT', 'INTC', 'BTC-USD', 'ETH-USD', 'RUB=X']
    max_dates = {}

    logging.info("Выполняется подключение к БД и загрузка последних дат по тикерам")

    try:
        with engine.connect() as conn:
            query = text("""
                SELECT ticker, MAX(price_date) as max_date
                FROM stock_market.prices_yfinance 
                WHERE ticker IN :tickers
                GROUP BY ticker;
            """)
            result = conn.execute(query, {'tickers': tuple(tickers_list)})
            max_dates = {row.ticker: row.max_date for row in result}
        logging.info("Последние даты из 'prices_yfinance': %s", max_dates)
    except Exception as e:
        logging.error("Ошибка при получении последних дат из БД: %s", e)
        raise

    earliest_max_date = min(max_dates.values()) if max_dates else date(2020, 1, 1)
    start_date_load = earliest_max_date - timedelta(days=14)

    logging.info("Загружаются данные из yfinance")
    df_raw = yf.download(
        tickers=tickers_list, 
        start=start_date_load, 
        end=date.today(), 
        interval='1d', 
        group_by='ticker',
        auto_adjust=True
    )
    
    if df_raw.empty:
        logging.warning("yfinance не вернул данных для указанного периода.")
        return

    # --- 5. Трансформация данных (wide-to-long) ---
    df_long = (
        df_raw.stack(level=0) # Переворачиваем мульти-индекс
        .rename_axis(['Date', 'ticker']) # Даем имена индексам
        .reset_index() # Превращаем индексы в колонки
    )
    # Оставляем только цену закрытия и переименовываем
    df_long = df_long[['Date', 'ticker', 'Close']].rename(columns={'Date': 'price_date', 'Close': 'price'})
    df_long.dropna(subset=['price'], inplace=True) # Удаляем строки без цены

    logging.info("Заполняем пропуски в данных")

    if df_long.empty:
        logging.warning("После первоначальной обработки не осталось данных для заполнения дат.")
        return
        
    final_df_filled = fill_missing_dates(df_long, 'price_date', 'ticker', 'price')
    
    logging.info("Фильтрация данных")

    rows_to_load = []
    for ticker_symbol in tickers_list:
        last_known_date = max_dates.get(ticker_symbol)
        ticker_data = final_df_filled[final_df_filled['ticker'] == ticker_symbol]
        
        if last_known_date:
            # Сравниваем объекты date, а не datetime
            new_data = ticker_data[pd.to_datetime(ticker_data['price_date']).dt.date > last_known_date]
        else:
            new_data = ticker_data
        
        if not new_data.empty:
            rows_to_load.append(new_data)
            
    if not rows_to_load:
        logging.info("После фильтрации новых данных для загрузки не осталось.")
        return

    df_to_load = pd.concat(rows_to_load, ignore_index=True)
    logging.info("Всего новых записей для загрузки (с учетом выходных): %d", len(df_to_load))
    
    try:
        df_to_load.to_sql(
            name='prices_yfinance',
            con=engine,
            schema='stock_market',
            if_exists='append',
            index=False,
            dtype={'price_date': Date(), 'ticker': VARCHAR(20), 'price': Numeric()}
        )
        logging.info("Данные из yfinance успешно загружены в таблицу 'prices_yfinance'.")
    except Exception as e:
        logging.error("Ошибка при загрузке данных yfinance в БД: %s", e)
        raise

