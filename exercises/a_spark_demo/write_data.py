"""
If you can read data from a file system, you can also write data to a file
system. The syntax is very symmetric.

The concept of partitions is introduced.
"""

from pathlib import Path

from pyspark.sql import SparkSession
import pyspark.sql.functions as psf

spark = SparkSession.builder.getOrCreate()

# Again, build a relative path, so it won't matter where this script resides.
csv_file_path = Path(__file__).parents[1] / "resources" / "foo.csv"


frame = spark.read.options(header="true", sep=";").csv(str(csv_file_path))

frame.printSchema()

better_frame = frame.withColumn("Date_of_Birth", psf.to_date("Date_of_Birth"))
better_frame.show()
better_frame.printSchema()
better_frame.repartition(3).write.parquet(
    str(csv_file_path.parent / "as_parquet"), mode="overwrite"
)
