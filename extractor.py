# Databricks notebook source
class Extract:
    def __init__(self):
        pass
    def extract(self):
        pass

class AirpodsAfterIPhoneExtractor(Extract):
    def __init__(self):
        # Initialize the spark session here
        self.spark = SparkSession.builder.getOrCreate()
    def extract(self):
        # Load CSV data
        transactionInputDF = self.spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/tables/Transaction_Updated.csv")
        # Show transaction data
        transactionInputDF.orderBy("customer_id", "transaction_date").show()
        # Load Delta table data
        customerInputDF = self.spark.read.format("delta").table("default.customer_delta")
        # Show customer data
        customerInputDF.show()
        
        InputDFs = {
            "transactionInputDF": transactionInputDF,
            "customerInputDF": customerInputDF
        }
        return InputDFs