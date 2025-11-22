import pandas as pd

import os
from typing import List
import hashlib

from ..strategy_builder.core.domain.enums import TimeFrameEnum


def list_files_in_folder(folder_path: str) -> List[str]:
    """List all files (not directories) in the given folder."""
    return [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if os.path.isfile(os.path.join(folder_path, f))
    ]



def get_historical_data(fetch_client, symbol, timeframe, date_today, start_date, end_date):
    logger.info(f"Fetching historical data for {symbol}:{timeframe} from {start_date} to {end_date}")

    data = fetch_client.get_symbol_data(
        symbol,
        timeframe=timeframe,  # e.g., "30" -> "M30"
        start=f"{start_date}T00:00:00Z",
        end=f"{end_date}T00:00:00Z"
    )
    df = pd.DataFrame(data)
    df["time"] = pd.to_datetime(df["time"])

    # Get the row with the max time
    max_time = df["time"].max()

    if max_time.date() == date_today:
        # Exclude the max time row
        df = df[df["time"] != max_time]

    return df

def fetch_historical_for_timeframe(fetch_client, symbol, mt5_tf: str, date_today, start_date, end_date):
    return get_historical_data(
        fetch_client,
        symbol,
        mt5_tf,
        date_today=date_today,
        start_date=start_date,
        end_date=end_date
    )

def get_stream_data(fetch_client, symbol, timeframe, nbr_bars=3):
    logger.info(f"fetching last {nbr_bars} data for {symbol}:{timeframe}")
    data = fetch_client.get_symbol_data(symbol, timeframe=timeframe, num_bars=nbr_bars)
    df = pd.DataFrame(data)
    df["time"] = pd.to_datetime(df["time"])
    return df


def has_new_candle(current_df, last_known_bar, candle_index):
    """Check if a new candle has closed."""
    if last_known_bar is None:
        return True  # Initial fetch
    current_latest_time = pd.to_datetime(current_df.iloc[-candle_index]["time"])
    last_known_time = pd.to_datetime(last_known_bar["time"])
    return current_latest_time > last_known_time



def generate_magic_number(
        strategy_name: str,
        symbol: str,
        timeframes: List[TimeFrameEnum],
        position_type:str = ""
) -> int:
    timeframe = "_".join(str(x) for x in timeframes)
    unique_str = f"{strategy_name}:{symbol}:{timeframe}:{position_type}"
    hash_object = hashlib.md5(unique_str.encode())
    hex_digest = hash_object.hexdigest()

    # Convert to integer and limit to 9 digits
    magic_number = int(hex_digest[:8], 16) % 1_000_000_000
    return magic_number


def create_folder(path:str):
    directory = os.path.dirname(path)
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)

def save_dataframe_to_parquet(df: pd.DataFrame, path:str, engine:str = "pyarrow"):
    create_folder(path)
    df.to_parquet(path, engine=engine, index=False)