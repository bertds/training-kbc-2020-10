"""
Illustrating how data can be read from files.
CSVs are ubiquitous and have many flavors, so the number of options for the CSV
reader is larger than that for other sources, like JSON and Parquet.
Also introducing the filter and orderBy methods.
When datasets get small enough that they might fit in the memory of the driver,
you can call collect or toPandas on them. From that moment on, you're no longer
working on distributed data.
"""

from pathlib import Path

import pyspark.sql.functions as psf
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

csv_file_path = Path(__file__).parents[1] / "resources" / "foo.csv"

# Python-esque:
frame = spark.read.csv(
    str(csv_file_path),
    sep=";",
    header=True,
)
# Scala-like:
frame = spark.read.options(  # With parentheses like this, you can add comments in long chains  # This would not have been possible with the backslash
    header="true", sep=";", inferSchema=True
).csv(
    str(csv_file_path)
)
print(frame.schema)
frame.show()
frame.printSchema()

frame.select("Age").withColumn("age_in_10_years", psf.col("age") + 10).show()

new_frame = frame.filter(frame.Age.isNotNull())

print(new_frame.count())

frame2 = frame.orderBy(psf.col("Age").asc()).cache()
frame2.show()
result = frame2.collect()
print(result)  # A list of Row objects
# Python is zero-based indexing: the zeroth element is the _first_ in an iterable
print(type(result[0]))
# Access row attributes dynamically
print(result[0].Name)
# Or using their position
print(result[0][0])
