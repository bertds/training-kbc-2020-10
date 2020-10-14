"""Illustrate how UDFs lose the generic character that their pure Python
counterparts exhibit.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, FloatType


def square(x):
    print(f"This is element: {x}")
    return x ** 2


spark = SparkSession.builder.getOrCreate()


def test_foo():
    return_datatype = IntegerType()  # FloatType()
    square_udf_int = udf(square, return_datatype)

    df = spark.createDataFrame([(1, 2.0), (3, 4.9), (5, 6.9)], schema=("a", "b"))

    df.select(
        "a",
        "b",
        square_udf_int("a").alias("a_squared"),
        square_udf_int("b").alias("b_squared"),
    ).show()
    # by default, pytest won't show the standard output stream when tests pass.
    # As we're interested in seeing _all_ the stdout (the print function call
    # inside the UDF), we explicitly make this test fail.
    assert False


def test_pure_python():
    print(square(2))
    print(square(2.0))
    print(square(2.5))
    # Same explanation as above: make the test fail.
    assert False
