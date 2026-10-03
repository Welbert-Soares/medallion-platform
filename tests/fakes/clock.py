from datetime import date, datetime, time, timezone


class FixedClock:
    """Clock frozen at a given instant (midnight UTC when given a date)."""

    def __init__(self, at: datetime | date):
        if not isinstance(at, datetime):
            at = datetime.combine(at, time(), tzinfo=timezone.utc)
        self.at = at

    def now(self) -> datetime:
        return self.at

    def today(self) -> date:
        return self.at.date()
