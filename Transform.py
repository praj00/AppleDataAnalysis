# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import lead,col,broadcast, collect_set,size,array_contains

class Transformer:
    def __init__(self):
        pass
    def transform(self,InputDFs):
        pass

class AirpodsAfterIphoneTransformer(Transformer):

    def transform(self,InputDfs):

        transactionInputDF=InputDfs.get("transactionInputDF")
        print("transactionInputDF in Transform")
        transactionInputDF.show()

        windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")

        transformedDF=transactionInputDF.withColumn(
            "next_product_name",lead("product_name").over(windowSpec)
        )

        print("Airpods after buying iphone")
        transformedDF.orderBy("customer_id","transaction_date","product_name").show()

        filteredDF=transformedDF.filter(
            #()
            (col("product_name")=='iPhone') & (col("next_product_name")=='AirPods')
        )

        filteredDF.orderBy("customer_id","transaction_date","product_name").show()

        customerInputDF=InputDfs.get("customerInputDF")

        customerInputDF.show()

        joinDF=customerInputDF.join(
            broadcast(filteredDF),
            "customer_id"
        )

        print("Joined DF")
        joinDF.show()

        return joinDF.select(
            "customer_id",
            "customer_name",
            "location"
        )


class OnlyAirpodsandIphone(Transformer):
    def transform(self,InputDFs):

        transactionInputDF=InputDFs.get("transactionInputDF")
        print("transactionInputDF in Transform")
        transactionInputDF.show()

        groupedDF = transactionInputDF.groupBy("customer_id").agg(
            collect_set("product_name").alias("products")
        )

        print("Grouped")
        groupedDF.show()


        
        filteredDF=groupedDF.filter(
            
            #()
            (array_contains(col("products"),'iPhone')) & (array_contains(col("products"),'AirPods')) & (size(col("products"))==2)
        )
        print("Only Airpods and iPhone")
        filteredDF.show()

        customerInputDF=InputDFs.get("customerInputDF")

        customerInputDF.show()

        joinDF=customerInputDF.join(
            broadcast(filteredDF),
            "customer_id"
        )

        print("Joined DF")
        joinDF.show()

        return joinDF.select(
            "customer_id",
            "customer_name",
            "location"
        )
