# Databricks notebook source
# MAGIC %run "./extractor"

# COMMAND ----------

# MAGIC %run "./Transform"

# COMMAND ----------

# MAGIC %run "./loader"

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("thebigdatashow.me").getOrCreate()
input_df=spark.read.format("csv").option("header","True").load("dbfs:/FileStore/tables/Transaction_Updated.csv")
input_df.show()

# COMMAND ----------

class firstWorkFlow:
    def __init__ (self):
        self.spark = SparkSession.builder.getOrCreate()
    def runner(self):
        #extract data from extractor file
        InputDFs= AirpodsAfterIPhoneExtractor().extract()
        #1st transformation from transform file
        #Customers who have bought airpods after buying iphone
        FirstTransformDF = AirpodsAfterIphoneTransformer().transform(InputDFs)
        #load te required data to given sink
        AirPodAfterIphoneLoader(FirstTransformDF).sink()



# COMMAND ----------

class secondWorkFlow:
    def __init__ (self):
        self.spark = SparkSession.builder.getOrCreate()
    def runner(self):
        #extract data from extractor file
        InputDFs= AirpodsAfterIPhoneExtractor().extract()
        #2nd transformation from transform file
        #Customers who have bought only airpods and iphone
        SecondTransformDF = OnlyAirpodsandIphone().transform(InputDFs)
        #load te required data to given sink
        OnlyAirPodAndIphoneLoader(SecondTransformDF).sink()



# COMMAND ----------

class WorkFlowRunner:
    def __init__(self,name):
        self.name=name
    def runner(self):
        if self.name=="firstWorkFlow":
            return firstWorkFlow().runner()
        elif self.name=="secondWorkFlow":
            return secondWorkFlow().runner()
        else:
            raise ValueError(f"Not implemented for{self.name}")

name="secondWorkFlow"

workFlowrunner= WorkFlowRunner(name).runner()

# COMMAND ----------

#dbutils.fs.mkdirs("dbfs:/FileStore/tables/apple_analysis/output/AirpodsAfterIphone")