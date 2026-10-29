from datetime import UTC, datetime


def utc_now() -> datetime:
    return datetime.now(UTC)


def format_timestamp(value: datetime) -> str:
    normalized = value.astimezone(UTC)
    return normalized.isoformat(timespec="milliseconds").replace("+00:00", "Z")
