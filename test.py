import unittest
from pyspark.sql import SparkSession
import mylib.lib as lib


class PySparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Initialize a Spark session
        cls.spark = (
            SparkSession.builder.master("local[*]").appName("PySparkTest").getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        # Stop the Spark session
        cls.spark.stop()

    # Test the load_data function
    def test_load_data(self):
        columns_to_select = [
            "ORDERNUMBER",
            "QUANTITYORDERED" "PRICEEACH",
            "ORDERLINENUMBER",
            "SALES",
            "ORDERDATE",
            "STATUS",
            "QTR_ID",
            "MONTH_ID",
            "YEAR_ID",
            "PRODUCTLINE",
            "MSRP",
            "PRODUCTCODE",
            "CUSTOMERNAME",
        ]
        # Path to the dataset
        test_data_path = "/FileStore/tables/sales_data_sample.csv"

        # Use the function to load the data
        df = lib.load_data(self.spark, test_data_path, columns=columns_to_select)

        # Check if the DataFrame is not empty
        self.assertFalse(df.head(1) == [])

        # Check if the correct columns are selected
        selected_columns = df.columns
        self.assertListEqual(selected_columns, columns_to_select)


if __name__ == "__main__":
    unittest.main()
