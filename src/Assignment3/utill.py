from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import current_date, date_sub, to_date
from datetime import datetime, timedelta

def process_user_activity():
    # Create Spark session
    spark = SparkSession.builder.appName("User Activity Log").getOrCreate()

    # Define the schema
    schema = StructType([
        StructField("log_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("user_activity", StringType(), True),
        StructField("time_stamp", TimestampType(), True)
    ])

    # Get the current date
    current_date_value = datetime.now()

    # Create the data
    data = [
        (1, 101, 'login', (current_date_value - timedelta(days=5))),
        (2, 101, 'login', (current_date_value - timedelta(days=4))),
        (3, 102, 'login', (current_date_value - timedelta(days=3))),
        (4, 103, 'login', (current_date_value - timedelta(days=2))),
        (5, 102, 'login', (current_date_value - timedelta(days=1))),
    ]

    # Create DataFrame
    df = spark.createDataFrame(data=data, schema=schema)

    # Filter past 7 days and count actions per user
    past_7days = df.filter(df['time_stamp'] >= date_sub(current_date(), 7)).groupBy('user_id').count()

    # Convert time_stamp to login_date with YYYY-MM-DD format
    df = df.withColumn('login_date', to_date(df['time_stamp']))

    # Write DataFrame to managed table
    df.write \
        .mode("overwrite") \
        .saveAsTable("user.login_details")

    # Write DataFrame to CSV file
    df.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv("user_activity_log.csv")

    # Show past 7 days activity
    past_7days.show(truncate=False)

# Driver code
if __name__ == "__main__":
    process_user_activity()
