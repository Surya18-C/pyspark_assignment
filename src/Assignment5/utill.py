# data_processing.py

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, lower, current_date

def create_dataframes():
    # Initialize Spark session
    spark = SparkSession.builder.appName("EmployeeData").getOrCreate()

    # Dataset1 - employee_df
    employee_schema = StructType([
        StructField("employee_id", IntegerType(), True),
        StructField("employee_name", StringType(), True),
        StructField("department", StringType(), True),
        StructField("state", StringType(), True),
        StructField("salary", IntegerType(), True),
        StructField("age", IntegerType(), True)
    ])

    employee_data = [
        (11, "james", "D101", "ny", 9000, 34),
        (12, "michel", "D101", "ny", 8900, 32),
        (13, "robert", "D102", "ca", 7900, 29),
        (14, "scott", "D103", "ca", 8000, 36),
        (15, "jen", "D102", "ny", 9500, 38),
        (16, "jeff", "D103", "uk", 9100, 35),
        (17, "maria", "D101", "ny", 7900, 40)
    ]

    employee_df = spark.createDataFrame(data=employee_data, schema=employee_schema)

    # Dataset2 - department_df
    department_schema = StructType([
        StructField("dept_id", StringType(), True),
        StructField("dept_name", StringType(), True)
    ])

    department_data = [
        ("D101", "sales"),
        ("D102", "finance"),
        ("D103", "marketing"),
        ("D104", "hr"),
        ("D105", "support")
    ]

    department_df = spark.createDataFrame(data=department_data, schema=department_schema)

    # Dataset3 - country_df
    country_schema = StructType([
        StructField("country_code", StringType(), True),
        StructField("country_name", StringType(), True)
    ])

    country_data = [
        ("ny", "newyork"),
        ("ca", "California"),
        ("uk", "Russia")
    ]

    country_df = spark.createDataFrame(data=country_data, schema=country_schema)

    return spark, employee_df, department_df, country_df

def process_data(spark, employee_df, department_df, country_df):
    # 2. Find average salary of each department
    employee_df.groupBy("department").avg("salary").show()

    # 3. Find employee's name and department name whose name starts with 'm'
    employee_df.join(department_df, employee_df.department == department_df.dept_id) \
        .filter(col("employee_name").startswith("m")) \
        .select("employee_name", "dept_name").show()

    # 4. Create a new column in employee_df as bonus (salary * 2)
    employee_df = employee_df.withColumn("bonus", col("salary") * 2)
    employee_df.show()

    # 5. Reorder the column names of employee_df
    employee_df = employee_df.select("employee_id", "employee_name", "salary", "state", "age", "department")
    employee_df.show()

    # 6. Inner join, left join, and right join with department_df
    # Inner Join
    inner_join_df = employee_df.join(department_df, employee_df.department == department_df.dept_id, "inner")
    inner_join_df.show()

    # Left Join
    left_join_df = employee_df.join(department_df, employee_df.department == department_df.dept_id, "left")
    left_join_df.show()

    # Right Join
    right_join_df = employee_df.join(department_df, employee_df.department == department_df.dept_id, "right")
    right_join_df.show()

    # 7. New DataFrame with country_name instead of State
    employee_with_country_df = employee_df.join(country_df, employee_df.state == country_df.country_code, "left") \
        .drop("state").withColumnRenamed("country_name", "state")
    employee_with_country_df.show()

    # 8. Convert all column names to lowercase and add load_date column
    for column in employee_with_country_df.columns:
        employee_with_country_df = employee_with_country_df.withColumnRenamed(column, column.lower())
    employee_with_country_df = employee_with_country_df.withColumn("load_date", current_date())
    employee_with_country_df.show()

    # 9. Create external tables in Parquet and CSV format
    # Save as Parquet
    employee_with_country_df.write.mode("overwrite").format("parquet").saveAsTable("employee_db.employee_parquet")

    # Save as CSV
    employee_with_country_df.write.mode("overwrite").format("csv").saveAsTable("employee_db.employee_csv")

    # Stop Spark session
    spark.stop()
