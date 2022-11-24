from datetime import datetime

def test_total_leads_gotten(total_leads):
    """Test that the total_leads_gotten output returns the correct data."""
    df, spark = total_leads
    assert df.count() == 11
    assert df.collect()[0]["MONTH"] == 1
    assert df.collect()[0]["TOTAL_LEADS_GOTTEN"] == 44444
    assert df.agg({'TOTAL_LEADS_GOTTEN': 'sum'}).collect()[0]["sum(TOTAL_LEADS_GOTTEN)"] == 476475

    spark.stop()




def test_last_10_days(last_10_days):
    """Test that the last_10_days output returns the correct data."""
    df, spark = last_10_days
    assert df.count() == 10
    assert df.collect()[0]["DATE"] == datetime(2022, 11, 14, 0, 0)
    assert df.collect()[0]["TOTAL_LEADS_GOTTEN"] == 1445
    assert df.agg({'TOTAL_LEADS_GOTTEN': 'sum'}).collect()[0]["sum(TOTAL_LEADS_GOTTEN)"] == 14425

    spark.stop()


def test_hq_staffs(hq_staffs):
    """Test that the hq_staff output returns the correct data."""
    df, spark = hq_staffs
    assert df.count() == 17740
    assert df.collect()[0]["COMPANY"] == "Smith PLC"
    assert df.collect()[0]["COUNT"] == 1138
    assert df.agg({'COUNT': 'sum'}).collect()[0]["sum(COUNT)"] == 264753

    spark.stop()


def test_leads_per_country(leads_per_country):
    """Test that the leads_per_country output returns the correct data."""
    df, spark = leads_per_country
    assert df.count() == 20
    assert df.collect()[0]["COUNTRY"] == "Congo"
    assert df.collect()[0]["COUNT"] == 8442
    assert df.agg({'COUNT': 'sum'}).collect()[0]["sum(COUNT)"] == 92105

    spark.stop()