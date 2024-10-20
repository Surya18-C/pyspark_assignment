from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("finding count").getOrCreate()
schema1 = StructType([StructField("customer", StringType(), nullable=False),
                      StructField("product_model", StringType(), nullable=False)
                      ])
data1= [("1", "iphone13"),
         ("1", "dell i5 core"),
         ("2", "iphone13"),
         ("2", "dell i5 core"),
         ("3", "iphone13"),
         ("3", "dell i5 core"),
         ("1", "dell i3 core"),
         ("1", "hp i5 core"),
         ("1", "iphone14"),
         ("3", "iphone14"),
         ("4", "iphone13")]
schema2 = StructType([
    StructField("product_model", StringType(), nullable=False)])
data2 = [("iphone13",),
         ("dell i5 core",),
         ("dell i3 core",),
         ("hp i5 core",),
         ("iphone14",)]
df1 = spark.createDataFrame(data=data1, schema=schema1)
df2 = spark.createDataFrame(data=data2, schema=schema2)
def only_product_iphone13(purchase_data_df):
    return purchase_data_df.groupBy("customer") \
            .agg({"product_model": "collect_set"}) \
            .filter("size(collect_set(product_model)) = 1 and collect_set(product_model)[0] = 'iphone13'") \
            .select("customer")


def iphone13_to_iphone14(purchase_data_df):
    iphone13 = purchase_data_df.filter("product_model = 'iphone13'").select("customer")
    iphone14 = purchase_data_df.filter("product_model = 'iphone14'").select("customer")
    return iphone13.intersect(iphone14)

def product_unique(df1,df2):
    count1 = df1.groupBy("customer").agg(countDistinct("product_model").alias("distinct_count"))
    count2 = df2.count()
    result=count1.filter(count1.distinct_count==count2)
    return result

#code