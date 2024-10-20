from pyspark_assignment.src.assignment1.util import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Purchase_analysis").getOrCreate()
result = only_product_iphone13(df1)
result.show()

result =iphone13_to_iphone14(df1)
result.show()
result_df1=product_unique(df1,df2)
result_df1.show()

#function