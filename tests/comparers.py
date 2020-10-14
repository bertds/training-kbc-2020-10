import logging

from pyspark.sql import DataFrame


def assert_frames_functionally_equivalent(
    df1: DataFrame, df2: DataFrame, check_nullability=True
):
    """Validate if 2 frames have identical schemas, and data, disregarding the
    ordering of both."""
    return False
