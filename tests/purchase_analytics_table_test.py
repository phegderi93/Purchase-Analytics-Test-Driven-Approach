import unittest
from pyspark.sql import SparkSession
from src.retail_model.purchase import Purchase
from src.streaming.purchase_analytics import PurchaseAnalytics
from datetime import datetime
import shutil
import os


class PurchaseAnalyticsTableTest(unittest.TestCase):
    lower_bound = 10.0
    upper_bound = 35.0
    tmp_dir = f"/tmp/spark-streaming-test"
    table_path = f"{tmp_dir}/purchase-analytics"

    @staticmethod
    def delete_tmp_dir(tmp_dir):
        if os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir)


    @classmethod
    def setUp(cls) -> None:
        cls.spark = SparkSession.builder \
            .appName("PurchaseAnalyticsTableTest") \
            .master("local[*]") \
            .getOrCreate()

    @classmethod
    def tearDown(cls) -> None:
        cls.spark.stop()
        cls.delete_tmp_dir(cls.tmp_dir)

    def test_filter_purchase(self):
        records = [
            {'purchaseId': 'c1', 'customerId': 'cust1', 'productId': 'prod1', 'quantity': 2, 'pricePerUnit': 15.0, 'purchaseTimestamp': datetime.fromisoformat('2023-10-01 10:00:00')},
            {'purchaseId': 'c2', 'customerId': 'cust1', 'productId': 'prod2', 'quantity': 1, 'pricePerUnit': 25.0, 'purchaseTimestamp': datetime.fromisoformat('2023-10-01 11:00:00')},
            {'purchaseId': 'c3', 'customerId': 'cust3', 'productId': 'prod3', 'quantity': 3, 'pricePerUnit': 5.0, 'purchaseTimestamp': datetime.fromisoformat('2023-10-01 12:00:00')},
            {'purchaseId': 'c4', 'customerId': 'cust4', 'productId': 'prod4', 'quantity': 4, 'pricePerUnit': 18.0, 'purchaseTimestamp': datetime.fromisoformat('2023-10-01 13:00:00')}
        ]

        expected = [
            {'purchaseId': 'c1', 'customerId': 'cust1', 'productId': 'prod1', 'quantity': 2, 'pricePerUnit': 15.0, 'purchaseTimestamp': datetime.fromisoformat('2023-10-01 10:00:00')},
            {'purchaseId': 'c2', 'customerId': 'cust1', 'productId': 'prod2', 'quantity': 1, 'pricePerUnit': 25.0,'purchaseTimestamp': datetime.fromisoformat('2023-10-01 11:00:00')},
            {'purchaseId': 'c3', 'customerId': 'cust3', 'productId': 'prod3', 'quantity': 3, 'pricePerUnit': 5.0, 'purchaseTimestamp': datetime.fromisoformat('2023-10-01 12:00:00')}
        ]

        # Create Parquet file and read it as streaming DataFrame
        streaming_df = self.get_streaming_df(records, Purchase.schema)



        actual_streaming_df = PurchaseAnalytics.filter_purchase(streaming_df, self.lower_bound, self.upper_bound )


        # Reading Spark Streaming DataFrame as Dictionary

        actual_dict, query = self.process_all_stream_as_dict(actual_streaming_df)

        self.assertEqual(len(expected), len(actual_dict))

        query.stop()


    def get_streaming_df(self, records, schema):
        df = self.spark.createDataFrame(records, schema=schema)

        df.write.mode("overwrite").format("parquet").save(self.table_path)

        streaming_df = self.spark.readStream.format("parquet").schema(Purchase.schema).load(self.table_path)
        return streaming_df

    def process_all_stream_as_dict(self,actual_streaming_df):
        query = actual_streaming_df.writeStream \
                .format("memory") \
                .queryName("filtered_purchases") \
                .outputMode("append") \
                .start()

        query.processAllAvailable()

        actual_df = self.spark.sql("Select * from filtered_purchases")

        actual_dict = [row.asDict() for row in actual_df.collect()]

        return actual_dict, query


