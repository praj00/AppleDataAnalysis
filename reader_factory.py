# Databricks notebook source

class DataSource:
    '''Abstract Class'''
    def __init__(self,path):
        self.path=path
    
    def get_data_frame(self):
        '''abstract method'''
        raise ValueError("Not implemented")

class CSVDataSource(DataSource):
    def get_data_frame(self):

        return (
            spark.
            read.
            format("csv").
            option("header",True).
            load(self.path)
        )

class ParquetDataSource(DataSource):
    def get_data_frame(self):

        return (
            spark.
            read.
            format("parquet").
            option("header",True).
            load(self.path)
        )

class DeltaDataSource(DataSource):
    def get_data_frame(self):

        return (
            spark.
            read.
            format("delta").
            option("header",True).
            load(self.path)
        )

def get_data_source(data_type,file_path):
    
    if data_type=='csv':
        return CSVDataSource(file_path)
    elif data_type=='parquet':
        return ParquetDataSource(file_path)
    elif data_type=='delta':
        return DeltaDataSource(file_path)
    else:
        raise ValueError (f"Not Implemented for Data Type:{data_type}")