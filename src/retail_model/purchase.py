from pyspark.sql.types import *


class Purchase:
    schema = StructType([
        StructField("purchaseId", StringType(), False),
        StructField("customerId", StringType(), False),
        StructField("productId", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("pricePerUnit", DoubleType(), False),
        StructField("purchaseTimestamp", TimestampType(), False)
    ])
