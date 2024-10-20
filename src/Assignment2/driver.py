# main.py
from pyspark.sql import SparkSession
from utils import process_credit_cards

# Initialize Spark session
spark = SparkSession.builder.appName("CreditCardMasking").getOrCreate()

# Call the function from utils.py to process the credit cards
result_df = process_credit_cards(spark)

# Show the result
result_df.show(truncate=False)

# Stop Spark session
spark.stop()
