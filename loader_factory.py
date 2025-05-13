# Databricks notebook source
class DataSink:
    def __init__(self, df, method, params, path):
        self.df = df
        self.path = path
        self.method = method
        self.params = params

    def load_data_frame(self):
        raise ValueError("Not Implemented")


class LoadToDBFS(DataSink):
    def load_data_frame(self):
        self.df.write.mode(self.method).save(self.path)


class LoadToDBFSWithPartition(DataSink):
    def load_data_frame(self):
        partitionByColumns = self.params.get("partitionByColumns")
        self.df.write.mode(self.method).partitionBy(*partitionByColumns).save(self.path)


class LoadToDeltaTable(DataSink):
    def load_data_frame(self):
        self.df.write.format("delta").mode(self.method).saveAsTable(self.path)


def get_sink_source(sink_type, df, path, method, params=None):
    if sink_type == "dbfs":
        return LoadToDBFS(df, method, params, path)
    elif sink_type == "dbfs_with_partition":
        return LoadToDBFSWithPartition(df, method, params, path)
    elif sink_type == "delta":
        return LoadToDeltaTable(df, method, params, path)
    else:
        raise ValueError(f"Not implemented for sink type: {sink_type}")
