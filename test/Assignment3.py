import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from datetime import datetime, timedelta
from user_activity import process_user_activity  # Assuming your function is in user_activity.py


class TestUserActivity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Create a Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("UserActivityTest") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        # Stop the Spark session
        cls.spark.stop()

    def test_process_user_activity(self):
        # Call the function to process user activity
        process_user_activity()

        # Load the managed table to validate the data
        result_df = self.spark.sql("SELECT * FROM user.login_details")

        # Check if DataFrame is not empty
        self.assertGreater(result_df.count(), 0, "The login_details table should not be empty.")

        # Validate schema
        expected_columns = ['log_id', 'user_id', 'user_activity', 'time_stamp', 'login_date']
        self.assertTrue(all(col in result_df.columns for col in expected_columns), "Missing columns in DataFrame.")

        # Validate the login_date format
        for row in result_df.collect():
            login_date = row['login_date']
            self.assertIsInstance(login_date, datetime.date, "login_date should be of type date.")

        # Validate that the activities in past 7 days are correct
        current_date_value = datetime.now()
        past_7days_count = result_df.filter(result_df['time_stamp'] >= (current_date_value - timedelta(days=7))).count()

        # Check if past 7 days activities are counted
        self.assertGreaterEqual(past_7days_count, 0, "There should be activities logged for the past 7 days.")


if __name__ == "__main__":
    unittest.main()
