from datetime import date
import datetime as dt

from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, StructField, StructType, StringType, BooleanType

from exercises.c_labellers.dates import (
    label_weekend,
    label_holidays3,
    is_belgian_holiday,
    label_holidays2,
)
from tests.comparers import assert_frames_functionally_equivalent

spark = SparkSession.builder.master("local[*]").getOrCreate()


def test_label_weekend():
    # make sure to explore
    # https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions

    expected = spark.createDataFrame(
        [(date(2018, 5, 12), "a", True), (date(2019, 5, 13), "b", False)],
        schema=StructType(
            [
                StructField("date", DateType(), True),
                StructField("foo", StringType(), True),
                StructField("is_weekend", BooleanType(), True),
            ]
        ),
    )

    frame_in = expected.select("date", "foo")

    actual = label_weekend(frame_in)
    assert_frames_functionally_equivalent(actual, expected)


def test_label_holidays():
    fields = [
        StructField("date", DateType(), False),
        StructField("foobar", StringType(), True),
        StructField("is_belgian_holiday", BooleanType(), True),
    ]
    input = spark.createDataFrame(
        [
            (date(2019, 1, 1), "foo"),  # New Year's
            (date(2019, 7, 21), "bar"),  # National holiday
            (date(2019, 12, 6), "fubar"),
        ],  # Saint-Nicholas
        schema=StructType(fields[:2]),
    )
    output = label_holidays2(input)
    output.show()
    expected = spark.createDataFrame(
        [
            (date(2019, 1, 1), "foo", True),  # New Year's
            (
                date(2019, 7, 21),
                "bar",
                True,
            ),  # National holiday
            (date(2019, 12, 6), "fubar", False),
        ],  # Saint-Nicholas
        schema=StructType(fields),
    )
    assert_frames_functionally_equivalent(output, expected, False)

    # Notes: this test highlights well that tests are a form of up-to-date documentation.
    # It also protects somewhat against future changes (what if someone changes the
    # label_holidays to reflect the holidays of France?
    # Additionally, with the test written out already, you do not have to go into
    # your main function and write `print` everywhere as you are developing it. In fact,
    # most likely, you would have written out at some point something similar to this test,
    # put it in __main__ and once you noticed it succeeded, you would 've removed it. That's
    # so unfortunate! The automated test would've been lost and you force someone else to
    # have to rewrite it.


def test_pure_python_function():
    # Tests that don't initialize a SparkSession finish almost instantly. Try
    # this test and compare it to any other test in this file to compare tests
    # that depend on a SparkSession.
    # For this reason, tests involving Spark are typically run less often than
    # tests without Spark, though a good Continuous Integration system will
    # still run _all_ tests, before merging to master!
    day1 = dt.date(2020, 7, 21)
    arbeid = dt.date(2020, 5, 1)
    arbeidplus = dt.date(2020, 5, 2)

    assert is_belgian_holiday(day1)
    assert is_belgian_holiday(arbeid)
    assert not is_belgian_holiday(arbeidplus)