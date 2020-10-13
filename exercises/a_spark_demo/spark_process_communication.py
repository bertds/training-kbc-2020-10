"""
Illustrating the creation of the sparkcontext (implicitly through the
SparkSession builder since Spark2.x) and which processes run where.

Also, touching on RDDs and the MapReduce concepts, though in practice you
should rarely use RDDs and both map and reduce have different names when you use
DataFrames.
"""
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, upper

spark = SparkSession.builder.getOrCreate()


df = spark.range(1)


t1 = time.time()
dataframe = spark.range(5).withColumn(
    "foo", lit("bar")
)  # .groupBy(col("id") % 10)  # transformatie
print(time.time() - t1)
dataframe.printSchema()
print(
    dataframe.count()
)  # This is an action: it triggers the executors to do work.
t2 = time.time()

print(t2 - t1)
dataframe.show()
#
#
df = spark.createDataFrame(
    data=[
        ("hello", "sunny", "world"),
        (None, "sunshine", "rainbows!"),
        ("One", "Two", "Three"),
        ("One", "Two", "Three"),
    ],
    schema=("a", "b", "c"),
)

df.show()
#
#
print(df.rdd.map(lambda row: row[1].upper()).collect())
#
#
def titular_mapping(row):
    print(
        "this is all executed on the workers, this is not the driver context"
    )
    return row[2].title(), row[1].lower()


#
#
print(df.rdd.map(titular_mapping).collect())
#
# # This is functionally the same as what you have on line 10 in this script, but
# # *much* more efficient, as it keeps the transformations in the Java VM.
df.select(upper(df["b"])).show()
