from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


class DateHelper:
    def __init__(self, date_format: str = "%Y-%m-%d"):
        self.fmt = date_format

    def get_day_of_week(self, date_str: str) -> str:
        """Return the day of the week for a given date."""
        date_obj = datetime.strptime(date_str, self.fmt)
        return date_obj.strftime('%A')

    def get_date_days_ago(self, n: int, from_date: str = None) -> str:
        """Return the date N days ago from today or a given date."""
        base_date = datetime.strptime(from_date, self.fmt) if from_date else datetime.today()
        result = base_date - timedelta(days=n)
        return result.strftime(self.fmt)

    def get_date_weeks_ago(self, n: int, from_date: str = None) -> str:
        """Return the date N weeks ago from today or a given date."""
        base_date = datetime.strptime(from_date, self.fmt) if from_date else datetime.today()
        result = base_date - timedelta(weeks=n)
        return result.strftime(self.fmt)

    def get_date_months_ago(self, n: int, from_date: str = None) -> str:
        """Return the date N months ago from today or a given date."""
        base_date = datetime.strptime(from_date, self.fmt) if from_date else datetime.today()
        result = base_date - relativedelta(months=n)
        return result.strftime(self.fmt)

    def get_today(self) -> str:
        """Return today's date as string."""
        return datetime.today().strftime(self.fmt)

    def is_new_day(self, threshold: str = "00:01") -> bool:
        """Check if current time matches a new day threshold (e.g. '00:01')."""
        current_time = datetime.now().astimezone().strftime("%H:%M")
        return current_time == threshold

