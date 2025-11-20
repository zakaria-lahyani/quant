import pandas as pd
from datetime import datetime, timedelta
from dateutil import parser, tz

from app.utils.logger import AppLogger

logger = AppLogger.get_logger("trade-restriction")

class TradeRestriction:
    def __init__(self, restriction_path: str, default_close_time_str: str, news_duration: int,
                 market_close_duration: int):
        self.restriction_path = restriction_path
        self.default_close_time_str = default_close_time_str
        self.news_duration = news_duration
        self.market_close_duration = market_close_duration

        self.news_file = "economic-calendar.csv"
        self.holidays_file = "holidays.csv"

    def is_news_block_active(self, current_time: datetime) -> bool:
        try:
            # Ensure current_time is timezone-aware
            if current_time.tzinfo is None:
                current_time = current_time.replace(tzinfo=tz.tzlocal())

            df = pd.read_csv(f"{self.restriction_path}/{self.news_file}")
            event_times = pd.to_datetime(
                df[df["Restrictions"] == 1]["Dates"], utc=True
            ).dt.tz_convert(current_time.tzinfo)
        except Exception as e:
            logger.error(f"Failed to load news events: {e}")
            return False

        for event_time in event_times:
            if event_time - timedelta(minutes=self.news_duration) <= current_time <= event_time + timedelta(
                    minutes=self.news_duration):
                logger.warning(f"[NEWS BLOCK ACTIVE] {event_time}")
                return True

        return False

    def is_market_closing_soon(self, instrument: str, current_time: datetime) -> bool:
        # Ensure current_time is timezone-aware
        if current_time.tzinfo is None:
            current_time = current_time.replace(tzinfo=tz.tzlocal())
            
        today_str = current_time.strftime("%Y-%m-%d")

        try:
            parsed_close_time = parser.isoparse(f"{today_str}T{self.default_close_time_str}")
            # Ensure parsed close time has timezone info
            if parsed_close_time.tzinfo is None:
                parsed_close_time = parsed_close_time.replace(tzinfo=current_time.tzinfo)
            else:
                parsed_close_time = parsed_close_time.astimezone(current_time.tzinfo)
            default_close_time = parsed_close_time
        except Exception as e:
            logger.error(f"Failed to parse default close time '{self.default_close_time_str}': {e}")
            return False

        try:
            df = pd.read_csv(f"{self.restriction_path}/{self.holidays_file}")
            df["Dates"] = pd.to_datetime(df["Dates"], utc=True).dt.tz_convert(current_time.tzinfo)

            df_today = df[(df["Instrument"] == instrument) & (df["Dates"].dt.strftime("%Y-%m-%d") == today_str)]
            print(df)
            print(df_today)
            print(default_close_time)

            if not df_today.empty:
                close_time = df_today.iloc[0]["Dates"]
                if close_time - timedelta(minutes=self.market_close_duration) <= current_time <= close_time + timedelta(
                        minutes=self.market_close_duration):
                    logger.warning(f"[MARKET CLOSING SOON - HOLIDAY] {instrument} closes at {close_time}")
                    return True
            else:
                if default_close_time - timedelta(
                        minutes=self.market_close_duration) <= current_time <= default_close_time + timedelta(
                    minutes=self.market_close_duration):
                    logger.warning(f"[MARKET CLOSING SOON - DEFAULT] {instrument} closes at {default_close_time}")
                    return True

        except Exception as e:
            logger.error(f"Failed to load holiday market close times: {e}")
            # Use the default_close_time we calculated earlier
            try:
                if default_close_time - timedelta(
                        minutes=self.market_close_duration) <= current_time <= default_close_time + timedelta(
                    minutes=self.market_close_duration):
                    logger.warning(f"[MARKET CLOSING SOON - FALLBACK] {instrument} closes at {default_close_time}")
                    return True
            except Exception as fallback_error:
                logger.error(f"Failed to check fallback close time: {fallback_error}")
                return False

        return False