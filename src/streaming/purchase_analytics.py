from pyspark.sql.functions import *


class PurchaseAnalytics:

    @staticmethod
    def filter_purchase(df, lower_bound, upper_bound):
        total_price = df['quantity'] * df['pricePerUnit']
        filter_condition = (total_price >= lower_bound) & (total_price <= upper_bound)

        return df.filter(filter_condition)


        return df
