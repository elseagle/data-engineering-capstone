from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

import logging
from datetime import datetime
from utils.helpers import (
    connect_sheet,
    strip_fields,
    null_validation,
    datetime_type_validation,
    localize_timestamp,
    convert_timestamp,
    save_as_parquet,
)

log = logging.getLogger(__name__)


def get_leads():
    """Get leads from the Google Sheet."""

    import pandas as pd
    from gspread.exceptions import APIError

    try:
        sheet = connect_sheet().get_all_records()
        df = pd.DataFrame(sheet)
        df.columns = [
            "company",
            "first_name",
            "last_name",
            "title",
            "sales_nav_url",
            "linkedin_url",
            "operator_timestamp",
        ]
    except APIError:
        df = pd.read_json(Variable.get("INPUT_FILE_PATH"))

    df = strip_fields(df)

    df["operator_timestamp"] = pd.to_datetime(df["operator_timestamp"])
    df["operator_timestamp"] = localize_timestamp(
        df["operator_timestamp"], "Africa/Lagos"
    )
    df["operator_timestamp"] = convert_timestamp(df["operator_timestamp"], "UTC")

    df.drop(["sales_nav_url", "linkedin_url"], axis=1, inplace=True)

    # null validation
    null_validations = null_validation(df)
    if null_validations:
        log.warning(f"Failed Vaildations on: {null_validations}")
        return

    # datetime validation
    dt_validation = datetime_type_validation(df)
    if dt_validation:
        log.waring("Datetime validation failed on: operator_timestamp")
        return

    df.to_csv("lead_export.csv", header=True, index=False)
    log.info(f"Dumped data at {datetime.now()}")


def load_leads_sql():
    """Load leads to the database."""

    pg_hook = PostgresHook(postgres_conn_id=Variable.get("POSTGRES_CONN_ID"))
    pg_hook.run("DELETE FROM leads")
    pg_hook.copy_expert(
        sql="COPY leads FROM STDIN WITH CSV HEADER",
        filename="lead_export.csv",
    )
    log.info(f"Loaded data at {datetime.now()}")


def load_leads_to_parquet():
    from pyspark.sql import SparkSession
    import pyspark.sql.types as T
    import pyspark.sql.functions as F
    import json

    spark = SparkSession.builder.appName(
        "Spark job for loading files into parquet"
    ).getOrCreate()

    leads_df = (
        spark.read.option("inferSchema", "true")
        .option("header", "true")
        .format("csv")
        .load("leads_export.csv")
    )

    companies_df = (
        spark.read.option("inferSchema", "true")
        .option("header", "true")
        .format("csv")
        .load("company_export.csv")
    )

    # sort by timestamp
    leads_df = leads_df.sort("operator_timestamp")

    # remove duplicates on constraint fields
    leads_df = leads_df.dropDuplicates(
        ["company", "first_name", "last_name", "country"]
    )
    companies_df = companies_df.dropDuplicates(["company", "hq_location"])

    # create tempView

    leads_df.createGlobalTempView("leads")
    companies_df.createGlobalTempView("companies")

    total_leads_gotten_by_year = spark.sql(
        """

                SELECT MONTH(operator_timestamp) MONTH, COUNT(*) TOTAL_LEADS_GOTTEN
                FROM global_temp.leads
                WHERE YEAR(operator_timestamp)=YEAR(NOW())
                GROUP BY MONTH(operator_timestamp)
                ORDER BY 1


                """
    )
    last_10_days = spark.sql(
        """

    SELECT operator_timestamp DATE, COUNT(*) TOTAL_LEADS_GOTTEN
    FROM global_temp.leads
    WHERE operator_timestamp >= NOW() - INTERVAL '10' days 
    AND operator_timestamp < NOW()
    GROUP BY 1
    ORDER BY 1


    """
    )

    leads_per_country = spark.sql(
        """

    SELECT country COUNTRY, COUNT(*) COUNT
    FROM global_temp.leads
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 20


    """
    )

    hq_staffs = spark.sql(
        """

    SELECT c.company COMPANY, COUNT(*) COUNT
    FROM global_temp.leads l
    LEFT JOIN global_temp.companies c ON c.hq_location = l.country AND c.company = l.company

    WHERE c.company IS NOT NULL

    GROUP BY 1
    ORDER BY 2 DESC


    """
    )

    save_as_parquet(
        total_leads_gotten_by_year, "output/total_leads_gotten_by_year.parquet"
    )
    save_as_parquet(last_10_days, "output/last_10_days.parquet")
    save_as_parquet(leads_per_country, "output/leads_per_country.parquet")
    save_as_parquet(hq_staffs, "output/hq_staffs.parquet")
