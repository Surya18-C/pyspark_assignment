import unittest
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

# Assuming the function is defined in a file named credit_card_processing.py
from credit_card_processing import process_credit_cards

class TestCreditCardProcessing(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Create a Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("CreditCardProcessingTest") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        # Stop the Spark session
        cls.spark.stop()

    def test_mask_card_number(self):
        expected_data = [
            ("1234567891234567", "************5677"),
            ("5678912345671234", "************1234"),
            ("9123456712345678", "************5678"),
            ("1234567812341122", "************1122"),
            ("1234567812341342", "************1342")
        ]
        expected_columns = ["card_number", "Masked_Card_Number"]

        # Call the function to process credit cards
        result_df: DataFrame = process_credit_cards(self.spark)

        # Collect the result and expected data
        result_data = [(row.card_number, row.Masked_Card_Number) for row in result_df.collect()]

        # Assert that the result matches the expected data
        self.assertEqual(result_data, expected_data, "The masked card numbers should match the expected values.")

if __name__ == "__main__":
    unittest.main()
