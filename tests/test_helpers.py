import pandas as pd
import pytz
from dags.utils.helpers import (
    localize_timestamp,
    convert_timestamp,
    strip_fields,
    null_validation,
    datetime_type_validation,
)


def test_localize_timestamp():
    """Test that the localize_timestamp function returns a localized timestamp."""

    df = pd.DataFrame({"date": ["2021-01-01 12:00:00", "2021-01-01 12:00:00"]})
    df["date"] = pd.to_datetime(df["date"])
    df["date"] = localize_timestamp(df["date"], "Africa/Lagos")
    assert df["date"].dt.tz == pytz.timezone("Africa/Lagos")


def test_convert_timestamp():
    """Test that the convert_timestamp function returns a UTC timestamp."""

    df = pd.DataFrame({"date": ["2021-01-01 12:00:00", "2021-01-01 12:00:00"]})
    df["date"] = pd.to_datetime(df["date"])
    df["date"] = localize_timestamp(df["date"], "Africa/Lagos")
    df["date"] = convert_timestamp(df["date"], "UTC")
    assert df["date"].dt.tz == pytz.timezone("UTC")


def test_strip_fields():
    """Test that the strip_fields function returns a dataframe with stripped fields."""

    df = pd.DataFrame(
        {
            "company": ["  Company 1  ", "  Company 2  "],
            "first_name": ["  First Name 1  ", "  First Name 2  "],
            "last_name": ["  Last Name 1  ", "  Last Name 2  "],
            "title": ["  Title 1  ", "  Title 2  "],
            "operator_timestamp": ["2021-01-01 12:00:00", "2021-01-01 12:00:00"],
        }
    )
    df = strip_fields(df)
    assert df["company"].iloc[0] == "Company 1"


def test_null_validation():
    """Test that the null_validation function returns a list of columns with null values."""

    df = pd.DataFrame(
        {
            "company": ["Company 1", "Company 2"],
            "first_name": ["First Name 1", "First Name 2"],
            "last_name": ["Last Name 1", "Last Name 2"],
            "title": ["Title 1", "Title 2"],
            "operator_timestamp": ["2021-01-01 12:00:00", "2021-01-01 12:00:00"],
        }
    )
    null_validations = null_validation(df)
    assert null_validations == []


def test_datetime_type_validation():
    """Test that the datetime_type_validation function returns a list of columns with invalid datetime values."""

    df = pd.DataFrame(
        {
            "company": ["Company 1", "Company 2"],
            "first_name": ["First Name 1", "First Name 2"],
            "last_name": ["Last Name 1", "Last Name 2"],
            "title": ["Title 1", "Title 2"],
            "operator_timestamp": ["2021-01-01 12:00:00", "2021-01-01 12:00:00"],
        }
    )
    dt_validation = datetime_type_validation(df)
    assert dt_validation is True
