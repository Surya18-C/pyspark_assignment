from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, explode_outer, posexplode, current_date, year, month, day
from pyspark.sql.types import StructType, ArrayType, StringType, IntegerType, StructField, MapType

json_path = r'C:\Users\Surya.C\OneDrive\Documents\ass4.json'
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("properties", StructType([
        StructField("name", StringType(), True),
        StructField("storeSize", StringType(), True)
    ]), True),
    StructField("employees", ArrayType(StructType([
        StructField("empId", IntegerType(), True),
        StructField("empName", StringType(), True)
    ])), True)
])
def spark_session():
    spark = SparkSession.builder.appName("spark-assignment").getOrCreate()
    return spark

def create_df(spark, data, schema):
    df = spark.createDataFrame(data, schema)
    return df

# 1. Read JSON file provided in the attachment using the dynamic function
def read_json(spark, path):
    df = spark.read.json(path, multiLine=True)
    return df

# 2. flatten the data frame which is a custom schema
def flatten_df(df):
    flatten_json_df = df.select("*", explode("employees").alias("employee")) \
        .select("*", "employee.empId", "employee.empName") \
        .select("*", "properties.name", "properties.storeSize").drop("properties", "employees", "employee")
    return flatten_json_df


def count_before_after_flatten(df, flattened_df):
    print("\nBefore Flatten: ", end="")
    print(df.count())
    print("\nAfter Flatten: ", end="")
    print(flattened_df.count())


def diff_explode_outer_posexplode(spark):
    data = [
        (1, [1, 2, 3]),
        (2, [4, None, 6]),
        (3, [])
    ]
    df = spark.createDataFrame(data, ["id", "numbers"])
    print("Original DataFrame:")
    df.show()
    exploded_df = df.select("id", explode("numbers").alias("number"))
    print("Exploded DataFrame:")
    exploded_df.show()
    exploded_outer_df = df.select("id", explode_outer("numbers").alias("number"))
    print("Exploded Outer DataFrame:")
    exploded_outer_df.show()
    pos_exploded_df = df.select("id", posexplode("numbers").alias("pos", "number"))
    print("PosExploded DataFrame:")
    pos_exploded_df.show()

# 5. Filter the id which is equal to 1001
def filter_employee_with_id(df, id):
    return df.filter(df['empId'] == id)

# 6. convert the column names from camel case to snake case
def toSnakeCase(dataframe):
    for column in dataframe.columns:
        snake_case_col = ''
        for char in column:
            if char.isupper():
                snake_case_col += '_' + char.lower()
            else:
                snake_case_col += char
        dataframe = dataframe.withColumnRenamed(column, snake_case_col)
    return dataframe

def add_load_date_with_current_date(df):
    result = df.withColumn("load_date", current_date())
    return result


def add_year_month_day(df):
    year_month_day_df = df.withColumn("year", year(df.load_date)) \
        .withColumn("month", month(df.load_date)) \
        .withColumn("day", day(df.load_date))
    return year_month_day_df

#code