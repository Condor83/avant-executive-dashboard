"""Denver business-day boundary and DST regression tests."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from analytics.yield_engine import denver_business_bounds_utc, denver_business_date_for_timestamp


def test_denver_business_bounds_dst_spring_forward() -> None:
    start_utc, end_utc = denver_business_bounds_utc(date(2026, 3, 8))

    assert start_utc == datetime(2026, 3, 8, 7, 0, tzinfo=UTC)
    assert end_utc == datetime(2026, 3, 9, 6, 0, tzinfo=UTC)
    assert end_utc - start_utc == timedelta(hours=23)


def test_denver_business_bounds_dst_fall_back() -> None:
    start_utc, end_utc = denver_business_bounds_utc(date(2026, 11, 1))

    assert start_utc == datetime(2026, 11, 1, 6, 0, tzinfo=UTC)
    assert end_utc == datetime(2026, 11, 2, 7, 0, tzinfo=UTC)
    assert end_utc - start_utc == timedelta(hours=25)


def test_denver_business_date_mapping_around_midnight_boundary() -> None:
    assert denver_business_date_for_timestamp(datetime(2026, 3, 8, 6, 59, 59, tzinfo=UTC)) == date(
        2026, 3, 7
    )
    assert denver_business_date_for_timestamp(datetime(2026, 3, 8, 7, 0, 0, tzinfo=UTC)) == date(
        2026, 3, 8
    )
