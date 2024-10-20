
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def process_credit_cards(spark):
    # Sample data
    data = [("1234567891234567",),
            ("5678912345671234",),
            ("9123456712345678",),
            ("1234567812341122",),
            ("1234567812341342",)]
    columns = ["card_number"]

    # Create DataFrame
    credit_card_df = spark.createDataFrame(data, columns)


    increase_partition = credit_card_df.rdd.repartition(5)
    decrease_partition = credit_card_df.rdd.coalesce(increase_partition.getNumPartitions())

    # Function to mask card numbers
    def mask_card_number(card_number):
        return "*" * (len(card_number) - 4) + card_number[-4:]

    mask_udf = udf(mask_card_number, StringType())


    result_df = credit_card_df.withColumn('Masked_Card_Number', mask_udf('card_number'))


    return result_df
