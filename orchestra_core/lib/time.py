"""Date helpers used by the billing pipeline."""

from __future__ import annotations

import datetime as _dt

__all__ = ["month_end_utc"]


def month_end_utc(day: _dt.date | _dt.datetime) -> _dt.date:
    """
    Return the last calendar day of the month that *day* falls into (UTC).

    The billing tests only compare the **date** part, so we expose a
    light-weight helper that is good enough both for tests and production.
    """
    # promote datetime → date
    if isinstance(day, _dt.datetime):
        day = day.date()

    # jump to 1-st of next month, then step back one day
    first_next = (day.replace(day=1) + _dt.timedelta(days=32)).replace(day=1)
    return first_next - _dt.timedelta(days=1)
