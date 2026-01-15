import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

KYIV_TZ = ZoneInfo("Europe/Kyiv")
MINUTES_IN_DAY = 1440
SECONDS_IN_DAY = 86400
SECONDS_IN_HOUR = 3600
SECONDS_IN_MINUTE = 60


def minutes_to_time(minutes: int) -> str:
    h, m = divmod(minutes % MINUTES_IN_DAY, 60)
    return f"{h:02d}:{m:02d}"


def format_period(start: int, end: int) -> str:
    return f"{minutes_to_time(start)} - {minutes_to_time(end)}"


def format_duration(start: datetime, end: datetime) -> str:
    seconds = int((end - start).total_seconds())
    days, remainder = divmod(seconds, SECONDS_IN_DAY)
    hours, remainder = divmod(remainder, SECONDS_IN_HOUR)
    minutes, _ = divmod(remainder, SECONDS_IN_MINUTE)

    parts = []
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")

    return " ".join(parts) if parts else "less than a minute"


def format_duration_hours(minutes: int) -> str:
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours}h {mins}m" if mins else f"{hours}h"


def format_restoration_time(restoration_time: str) -> str:
    match = re.search(r"(\d{1,2}:\d{2})\s+(\d{2}\.\d{2}\.\d{4})", restoration_time)
    if not match:
        return restoration_time

    time_str = match.group(1)
    date_str = match.group(2)

    try:
        restoration_date = datetime.strptime(date_str, "%d.%m.%Y").date()
    except ValueError:
        return restoration_time

    now_kyiv = datetime.now(KYIV_TZ)
    today_date = now_kyiv.date()
    tomorrow_date = today_date + timedelta(days=1)

    if restoration_date == today_date:
        return time_str
    elif restoration_date == tomorrow_date:
        return f"tomorrow {time_str}"
    else:
        return f"{time_str} {date_str}"
