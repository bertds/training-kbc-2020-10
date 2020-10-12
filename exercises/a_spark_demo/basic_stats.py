"""
Introducing `DataFrame.describe` as a means to get simple statistics. Note that
the mean and stddev are not specified for columns of datatypes where that does
not make sense, like strings and dates. Also observe that `count` gives the
non-null count.
"""

from pathlib import Path

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Use a relative path to the resources folder, so that this script will run
# regardless from its location on the file system.
csv_file_path = Path(__file__).parents[1] / "resources" / "foo.csv"

# Python-esque:
frame = spark.read.csv(str(csv_file_path), sep=";", header=True)
frame.show()
frame.printSchema()

print(frame.schema)
print(frame.columns)
frame.describe().show(truncate=False)
