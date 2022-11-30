from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.types import StringType, DateType, FloatType, TimestampType, IntegerType, StructType
from pyspark.sql.functions import to_date, to_timestamp, split, explode, posexplode, trim, sum, col, size, current_date

conf = SparkConf() \
    .setAppName("my_etl")

spark = SparkSession.builder \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()


def extract():
    input_schema = StructType() \
        .add("time_stamp", StringType()) \
        .add("your_name", StringType()) \
        .add("item", StringType()) \
        .add("date_of_purchase", StringType()) \
        .add("price", FloatType()) \
        .add("for_whom", StringType())

    input_df = spark.read \
        .format("csv") \
        .schema(input_schema) \
        .option("header", True) \
        .option("path", "siva/datasets/input/nov_room_bill.csv") \
        .load()

    return input_df


def transform(input_df):
    # Modifying dates
    date_mod = input_df.withColumn("time_stamp", to_timestamp("time_stamp", "dd-MM-yyyy HH:mm")) \
        .withColumn("date_of_purchase", to_date("date_of_purchase", "dd-MM-yyyy"))

    # Explode expences
    explode_values = date_mod.withColumn("for_whom", split("for_whom", ",")) \
        .withColumn("number_of_persons", size(col("for_whom"))) \
        .withColumn("for_whom", explode("for_whom")) \
        .withColumn("for_whom", trim("for_whom")) \
        .withColumn("share_of_each_person", col("price") / col("number_of_persons")) \
        .filter("your_name != for_whom").orderBy(col("your_name"), col("for_whom"))

    # Aggregate expences
    agg_amounts = explode_values.groupBy("your_name", "for_whom") \
        .agg(sum("share_of_each_person").alias("amount"))

    # Rename columns
    agg_amounts.withColumnRenamed("your_name", "payer") \
        .withColumnRenamed("for_whom", "receiver").orderBy(col("your_name"), col("for_whom")) \
        .withColumn("inserted_date", current_date()).show(truncate=False, n=100000)

    return input_df


def load(input_df):
    pass


if __name__ == "__main__":
    load(transform(extract()))

print("load success")