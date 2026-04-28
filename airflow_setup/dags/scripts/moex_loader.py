import os
import pandas as pd
from moexalgo import Ticker, session
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
        full_range = pd.date_range(start=min_date, end=max_date, freq='D')
        
        resampled_df = ticker_df.reindex(full_range).ffill().bfill()
        resampled_df[ticker_col] = ticker_name
        all_filled_dfs.append(resampled_df)
        
    if not all_filled_dfs:
        return pd.DataFrame()

    final_df = pd.concat(all_filled_dfs).reset_index().rename(columns={'index': date_col})
    logging.info("Заполнение пропущенных дат завершено.")
    return final_df

def load_moex_data_to_db():
    """
    Загружает дневные котировки для тикеров MOEX, заполняет пропуски
    в датах, оставляет только новые данные и загружает их в таблицу 'prices'.
    """
    load_dotenv()
    
    api_token = os.getenv('MOEX_API_TOKEN'); session.TOKEN = api_token
    db_url = os.getenv('DATABASE_URL'); engine = create_engine(db_url)
    
    tickers_list = ['SBER', 'GAZP', 'LKOH', 'YDEX', 'T'] #менем YNDX на YDEX, а TCSG
    max_dates = {}

    logging.info("Выполняется подключение к БД и загрузка последних дат по тикерам")

    try:
        with engine.connect() as conn:
            query = text("SELECT ticker, MAX(price_date) as max_date FROM stock_market.prices_moex WHERE ticker IN :tickers GROUP BY ticker;")
            result = conn.execute(query, {'tickers': tuple(tickers_list)})
            max_dates = {row.ticker: row.max_date for row in result}
    except Exception as e:
        logging.error("Ошибка при получении последних дат из БД: %s", e)
        raise

    earliest_max_date = min(max_dates.values()) if max_dates else date(2020, 1, 1)
    start_date_load = earliest_max_date - timedelta(days=14)

    logging.info("Получаем данные по тикерам от MOEX")

    all_tickers_data = []
    for ticker_symbol in tickers_list:
        try:
            ticker_obj = Ticker(ticker_symbol)
            df = ticker_obj.candles(start=start_date_load, end=date.today(), period='1d')
            df['ticker'] = ticker_symbol
            df.reset_index(inplace=True)
            df.rename(columns={'begin': 'price_date', 'close': 'price'}, inplace=True)
            df = df[['price_date', 'ticker', 'price']]
            df['price_date'] = pd.to_datetime(df['price_date'])
            all_tickers_data.append(df)
        except Exception as e:
            logging.error("Не удалось загрузить данные для %s: %s", ticker_symbol, e)

    if not all_tickers_data:
        logging.warning("Новых данных для загрузки нет.")
        return

    final_df = pd.concat(all_tickers_data, ignore_index=True)
    
    final_df_filled = fill_missing_dates(final_df, 'price_date', 'ticker', 'price')


    rows_to_load = []
    for ticker_symbol in tickers_list:
        last_known_date = max_dates.get(ticker_symbol)
        ticker_data = final_df_filled[final_df_filled['ticker'] == ticker_symbol]
        
        if last_known_date:
            new_data = ticker_data[ticker_data['price_date'].dt.date > last_known_date]
        else:
            new_data = ticker_data # Если тикер новый, берем все
        
        if not new_data.empty:
            rows_to_load.append(new_data)
            
    if not rows_to_load:
        logging.info("После фильтрации новых данных для загрузки не осталось.")
        return

    # Собираем итоговый DataFrame для загрузки
    df_to_load = pd.concat(rows_to_load, ignore_index=True)
    logging.info("Всего новых записей для загрузки (с учетом выходных): %d", len(df_to_load))

    # грузим в бд
    try:
        df_to_load.to_sql(
            name='prices_moex',
            con=engine,
            schema='stock_market',
            if_exists='append',
            index=False,
            dtype={'price_date': Date(), 'ticker': VARCHAR(20), 'price': Numeric()}
        )
        logging.info("Данные по тикерам MOEX успешно загружены в таблицу 'prices_moex'.")
    except Exception as e:
        logging.error("Ошибка при загрузке данных MOEX в БД: %s", e)
        raise

