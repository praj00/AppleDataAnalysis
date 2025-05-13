# Databricks notebook source
# MAGIC %run "./loader_factory"

# COMMAND ----------

class AbstractLoader:
    def __init__(self, transformedDF):
        self.transformedDF = transformedDF

    def sink(self):
        pass

class AirPodAfterIphoneLoader(AbstractLoader):
    def sink(self):
        get_sink_source(
            sink_type="dbfs",
            df=self.transformedDF,
            path="dbfs:/FileStore/tables/apple_analysis/output/AirpodsAfterIphone",
            method='overwrite'
        ).load_data_frame()

class OnlyAirPodAndIphoneLoader(AbstractLoader):
    def sink(self):
        params = {
            "partitionByColumns":["location"]
        }
        get_sink_source(
            sink_type="dbfs_with_partition",
            df=self.transformedDF,
            path="dbfs:/FileStore/tables/apple_analysis/output/AirpodsOnlyIphone",
            method='overwrite',
            params=params
        ).load_data_frame()

        get_sink_source(
            sink_type="delta",
            df=self.transformedDF,
            path="default.onlyAirpodsAndIphone",
            method='append',
            params=params
        ).load_data_frame()