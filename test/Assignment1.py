import unittest
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

# Assuming the functions are defined in a file named product_functions.py
from product_functions import only_product_iphone13, iphone13_to_iphone14, product_unique

class TestProductFunctions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Create a Spark session for testing
        cls.spark = SparkSession.builder.appName("ProductFunctionsTest").getOrCreate()

        # Sample data for DataFrame 1
        cls.schema1 = StructType([
            StructField("customer", StringType(), nullable=False),
            StructField("product_model", StringType(), nullable=False)
        ])
        cls.data1 = [
            ("1", "iphone13"),
            ("1", "dell i5 core"),
            ("2", "iphone13"),
            ("2", "dell i5 core"),
            ("3", "iphone13"),
            ("3", "dell i5 core"),
            ("1", "dell i3 core"),
            ("1", "hp i5 core"),
            ("1", "iphone14"),
            ("3", "iphone14"),
            ("4", "iphone13")
        ]
        cls.df1 = cls.spark.createDataFrame(data=cls.data1, schema=cls.schema1)

        # Sample data for DataFrame 2
        cls.schema2 = StructType([
            StructField("product_model", StringType(), nullable=False)
        ])
        cls.data2 = [
            ("iphone13",),
            ("dell i5 core",),
            ("dell i3 core",),
            ("hp i5 core",),
            ("iphone14",)
        ]
        cls.df2 = cls.spark.createDataFrame(data=cls.data2, schema=cls.schema2)

    @classmethod
    def tearDownClass(cls):
        # Stop the Spark session
        cls.spark.stop()

    def test_only_product_iphone13(self):
        expected_customers = ["1", "2", "4"]
        result_df: DataFrame = only_product_iphone13(self.df1)

        result_customers = [row.customer for row in result_df.collect()]
        self.assertEqual(sorted(result_customers), sorted(expected_customers), "The customers who only purchased iPhone13 should be correct.")

    def test_iphone13_to_iphone14(self):
        expected_customers = ["3"]
        result_df: DataFrame = iphone13_to_iphone14(self.df1)

        result_customers = [row.customer for row in result_df.collect()]
        self.assertEqual(sorted(result_customers), sorted(expected_customers), "The customers who purchased both iPhone13 and iPhone14 should be correct.")

    def test_product_unique(self):
        expected_count = 1  # Only customer '1' has 5 unique products
        result_df: DataFrame = product_unique(self.df1, self.df2)

        result_count = result_df.count()
        self.assertEqual(result_count, expected_count, "The number of customers with unique product count matching the total should be correct.")

if __name__ == "__main__":
    unittest.main()
cls