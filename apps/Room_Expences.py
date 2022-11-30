from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.types import StringType, DateType, FloatType, TimestampType, IntegerType, StructType
from pyspark.sql.functions import to_date, to_timestamp, split, explode, posexplode, trim, sum, col, size, current_date, \
    when, lit, date_format

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
        .filter("your_name != for_whom") \
        .withColumn("expense_month", date_format("date_of_purchase", 'MMM'))

    # Aggregate expences
    agg_amounts = explode_values.groupBy("your_name", "for_whom", "expense_month") \
        .agg(sum("share_of_each_person").alias("amount"))

    # Rename columns
    payer_receiver = agg_amounts.withColumnRenamed("your_name", "payer") \
        .withColumnRenamed("for_whom", "receiver")

    payer_receiver.select("payer", "receiver", "amount", "expense_month").orderBy(col("payer"), col("receiver")).show(
        truncate=False, n=100000)

    # Final payer
    payer_receiver_2 = payer_receiver.withColumnRenamed("payer", "payer_2") \
        .withColumnRenamed("receiver", "receiver_2") \
        .withColumnRenamed("amount", "amount_2") \
        .withColumnRenamed("expense_month", "expense_month_2")

    # Join dataframes
    join_condition = (payer_receiver.payer == payer_receiver_2.receiver_2) & (
                payer_receiver.receiver == payer_receiver_2.payer_2)
    join_type = "inner"

    join_df = payer_receiver.join(payer_receiver_2, join_condition, join_type)

    # Deriving final_payer
    final_payer_df = join_df.withColumn("final_payer", when(col("amount") > col("amount_2"), col("payer_2"))
                                        .when(col("amount_2") > col("amount"), col("payer"))
                                        .when(col("amount") == col("amount_2"), lit("No need to pay")))

    # Deriving final_amount_to_pay_for_final_payer
    final_amount_to_pay_for_final_payer_df = final_payer_df.withColumn("final_amount_to_pay",
                                                                       when(col("amount") > col("amount_2"),
                                                                            col("amount") - col("amount_2"))
                                                                       .when(col("amount_2") > col("amount"),
                                                                             col("amount_2") - col("amount"))
                                                                       .when(col("amount") == col("amount_2"), 0))

    # Deriving final_receiver
    final_receiver_df = final_amount_to_pay_for_final_payer_df.withColumn("final_receiver",
                                                                          when(col("amount") > col("amount_2"),
                                                                               col("payer"))
                                                                          .when(col("amount_2") > col("amount"),
                                                                                col("payer_2"))
                                                                          .when(col("amount") == col("amount_2"),
                                                                                lit("No need to pay"))).select(
        "final_payer", "final_receiver", "final_amount_to_pay", "expense_month").dropDuplicates()

    final_df = final_receiver_df.groupBy("final_payer", "final_receiver", "expense_month") \
        .agg(sum("final_amount_to_pay").alias("final_amount_to_pay")).coalesce(2) \
        .withColumnRenamed("final_receiver", "pay_to_this_person")

    final_df.select("final_payer", "pay_to_this_person", "final_amount_to_pay", "expense_month").orderBy(
        col("final_payer"), col("pay_to_this_person")).show(truncate=False, n=100000)
    # final_receiver_df.orderBy(col("final_payer"), col("final_receiver")).show(truncate=False, n=100000)

    # payer_receiver_2.show()

    # .join(payer_receiver.alias("payer_df_2"), ())

    # .show(truncate=False, n=100000)

    return input_df


def load(input_df):
    pass


if __name__ == "__main__":
    load(transform(extract()))
    spark.stop()
    print("load success")